"""
tce.py

Ingestão bronze/silver — TCE-SP AUDESP (licitações/contratos de obras).
Python puro (sem PySpark/Databricks), para rodar em VM Linux via cron.

Alinhado ao mesmo padrão arquitetural do pncp.py:
  - Bronze e silver vivem como UM ÚNICO arquivo Parquet cada, no Azure Blob
    Storage — não mais particionado por período em vários blobs.
  - Lidos/escritos via buffer em memória (BytesIO), nunca em disco.
  - O único estado persistido localmente é o checkpoint (quais períodos já
    foram processados com sucesso), sem dado de negócio dentro dele.
  - GARANTIA: um período só entra no checkpoint DEPOIS que o upload do lote
    correspondente é confirmado no Azure — nunca antes. "Está no checkpoint"
    sempre implica "o dado está salvo no Azure", mesmo que o processo seja
    interrompido no meio de um lote.
  - Silver é sempre regenerada por completo a partir do bronze corrente a
    cada execução (não incremental) — por isso não existe mais checkpoint
    separado para a silver, só para a bronze.

Saídas:
  {AZURE_STORAGE_CONTAINER}/{AZURE_BLOB_PREFIX}/bronze/tce_licitacoes_obras.parquet
  {AZURE_STORAGE_CONTAINER}/{AZURE_BLOB_PREFIX}/silver/tce_licitacoes_obras_consolidado.parquet

Uso:
  python tce.py --anos 2022 2023 2024 --modo append
  python tce.py --anos 2024 --modo overwrite
  python tce.py --skip-ingest                # só regenera a silver a partir do bronze existente
  python tce.py --anos 2024 --skip-upload     # teste pontual — checkpoint não avança, ver aviso

.env na raiz do projeto (um nível acima da pasta deste script), não versionar:
  AZURE_STORAGE_CONNECTION_STRING=...
  ou
  AZURE_STORAGE_ACCOUNT_NAME=...
  AZURE_STORAGE_ACCOUNT_KEY=...
  AZURE_STORAGE_CONTAINER=conteiner
  AZURE_BLOB_PREFIX=tce
"""

import argparse
import io
import json
import logging
import os
import tempfile
import time
import traceback
import zipfile
from datetime import date, datetime
from pathlib import Path

import pandas as pd
import requests
import zipfile_deflate64  # noqa: F401  (habilita suporte a deflate64 no zipfile)
from dotenv import load_dotenv

try:
    from azure.storage.blob import BlobServiceClient
except ImportError:
    BlobServiceClient = None

# ------------------------------------------------------------------
# Config
# ------------------------------------------------------------------
# .env fica na raiz do projeto (um nível acima da pasta do script), igual ao
# padrão usado no pncp.py — evita duplicar credenciais por pipeline.
BASE_DIR = os.path.dirname(os.path.abspath(__file__))  # .../tce/
ENV_PATH = os.path.normpath(os.path.join(BASE_DIR, "..", ".env"))
load_dotenv(ENV_PATH, override=True)

TCE_BASE = "https://transparencia.tce.sp.gov.br/sites/default/files/conjunto-dados/licitacoes-contratos"
DELAY_ENTRE_DOWNLOADS = 0.5  # segundos entre downloads, só por educação com o servidor

# ── Config Azure Blob Storage (tudo vem do .env, nunca hardcoded) ─────────
# Opção A (mais simples): AZURE_STORAGE_CONNECTION_STRING sozinha
# Opção B: AZURE_STORAGE_ACCOUNT_NAME + AZURE_STORAGE_ACCOUNT_KEY
# Em ambos os casos, AZURE_STORAGE_CONTAINER define o container de destino.
# AZURE_BLOB_PREFIX (não mais "AZURE_BLOB_PREFIX_tce") — mesmo nome de
# variável usado no pncp.py, pra evitar o tipo de inconsistência que já
# causou prefixo vazio silenciosamente em outro pipeline.
AZURE_CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
AZURE_ACCOUNT_NAME = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
AZURE_ACCOUNT_KEY = os.getenv("AZURE_STORAGE_ACCOUNT_KEY")
AZURE_CONTAINER = os.getenv("AZURE_STORAGE_CONTAINER", "conteiner")
AZURE_BLOB_PREFIX = os.getenv("AZURE_BLOB_PREFIX_tce", "tce").strip("/")

NOME_BRONZE = "tce_licitacoes_obras.parquet"
NOME_SILVER = "tce_licitacoes_obras_consolidado.parquet"

# Checkpoint local: guarda os períodos já confirmados no Azure. Único
# arquivo agora (não tem mais checkpoint separado pra silver, já que ela
# sempre reprocessa o bronze inteiro do zero a cada execução).
DATA_DIR = Path(os.path.normpath(os.path.join(BASE_DIR, "..", "data")))
CHECKPOINT_PATH = Path(os.getenv("CHECKPOINT_PATH", str(DATA_DIR / "checkpoint_tce.json")))

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("tce")

# O SDK do Azure loga cada request/response HTTP em nível INFO — polui o log.
for _azure_logger in ("azure", "azure.core.pipeline.policies.http_logging_policy"):
    logging.getLogger(_azure_logger).setLevel(logging.WARNING)

log.info(f".env carregado de: {ENV_PATH} (existe: {os.path.exists(ENV_PATH)})")
log.info(f"Container: {AZURE_CONTAINER} | prefixo: {AZURE_BLOB_PREFIX}")

COLUNAS = [
    "municipio", "entidade", "cod_licitacao", "modalidade",
    "objeto", "descricao_objeto", "produto_item",
    "qtd_contratada", "unidade_contratada",
    "vl_unit_orcamento_lote", "qtd_orcamento_lote", "un_orcamento_lote",
    "vl_unit_orcamento_item", "qtd_orcamento_item", "un_orcamento_item",
    "num_edital", "dt_edital",
    "cnpj_participante", "nome_participante",
    "resultado_habilitacao", "vl_proposta",
    "periodo", "dt_ingestao",
]


# ------------------------------------------------------------------
# Checkpoint local (só a bronze tem — silver é sempre full-reprocess)
# ------------------------------------------------------------------
def carregar_checkpoint() -> set:
    """Lê o checkpoint local. Retorna conjunto vazio se não existir ou estiver corrompido."""
    if not CHECKPOINT_PATH.exists():
        return set()
    try:
        with open(CHECKPOINT_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        return set(data.get("periodos_concluidos", []))
    except Exception as e:
        log.warning(f"Checkpoint '{CHECKPOINT_PATH}' ilegível ({type(e).__name__}) — iniciando do zero")
        return set()


def salvar_checkpoint(concluidos: set):
    """Grava o checkpoint de forma atômica (escreve em .tmp e renomeia),
    evitando corromper o arquivo se o processo for interrompido no meio da escrita."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    tmp_path = CHECKPOINT_PATH.with_suffix(".tmp")
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(
            {
                "periodos_concluidos": sorted(concluidos),
                "atualizado_em": datetime.now().isoformat(timespec="seconds"),
            },
            f, indent=2, ensure_ascii=False,
        )
    tmp_path.replace(CHECKPOINT_PATH)  # rename é atômico no mesmo filesystem


# ------------------------------------------------------------------
# Azure Blob Storage — bronze/silver como arquivo único, em memória
# ------------------------------------------------------------------
def _azure_configurado() -> bool:
    return bool(AZURE_CONNECTION_STRING) or bool(AZURE_ACCOUNT_NAME and AZURE_ACCOUNT_KEY)


def get_blob_service_client():
    """
    Retorna um BlobServiceClient configurado a partir do .env, ou None se a
    SDK não estiver instalada ou as credenciais não estiverem configuradas.
    Nunca levanta exceção — quem chama decide se aborta ou só avisa (mesmo
    padrão do pncp.py).
    """
    if BlobServiceClient is None:
        log.warning("Pacote 'azure-storage-blob' não instalado — pulando operação no Azure. "
                    "Instale com: pip install azure-storage-blob")
        return None
    if not _azure_configurado():
        log.warning(f"Nenhuma credencial do Azure encontrada no .env ({ENV_PATH}) — pulando operação. "
                    "Configure AZURE_STORAGE_CONNECTION_STRING (ou AZURE_STORAGE_ACCOUNT_NAME + "
                    "AZURE_STORAGE_ACCOUNT_KEY).")
        return None
    try:
        if AZURE_CONNECTION_STRING:
            return BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)
        account_url = f"https://{AZURE_ACCOUNT_NAME}.blob.core.windows.net"
        return BlobServiceClient(account_url=account_url, credential=AZURE_ACCOUNT_KEY)
    except Exception as e:
        log.error(f"Falha ao criar client do Azure Blob Storage: {type(e).__name__}: {e}")
        return None


def _blob_name(nome_arquivo: str, subpasta: str) -> str:
    partes = [p for p in (AZURE_BLOB_PREFIX, subpasta) if p]
    return "/".join(partes + [nome_arquivo]) if partes else nome_arquivo


def baixar_parquet_do_blob(nome_arquivo: str, subpasta: str) -> pd.DataFrame:
    """Baixa um blob parquet direto para a memória e retorna como DataFrame —
    nunca escreve nada em disco. DataFrame vazio se o blob não existir ou
    o Azure não estiver configurado."""
    client = get_blob_service_client()
    if client is None:
        return pd.DataFrame()

    blob_name = _blob_name(nome_arquivo, subpasta)
    try:
        container_client = client.get_container_client(AZURE_CONTAINER)
        blob_client = container_client.get_blob_client(blob_name)
        if not blob_client.exists():
            log.info(f"Nenhum parquet prévio em '{blob_name}' — começando do zero")
            return pd.DataFrame()
        buffer = io.BytesIO(blob_client.download_blob().readall())
        df = pd.read_parquet(buffer)
        log.info(f"Baixado '{blob_name}' da memória — {len(df):,} registro(s) prévio(s)")
        return df
    except Exception as e:
        log.error(f"Falha ao baixar '{blob_name}' do Azure: {type(e).__name__}: {e}")
        return pd.DataFrame()


def upload_dataframe_parquet(df: pd.DataFrame, nome_arquivo: str, subpasta: str = "") -> bool:
    """Serializa df como parquet num buffer em memória e sobe para o Azure,
    sobrescrevendo o blob. Nunca grava em disco. Retry de 3 tentativas com
    30s de espera (mesmo comportamento já validado na versão anterior)."""
    client = get_blob_service_client()
    if client is None:
        return False

    blob_name = _blob_name(nome_arquivo, subpasta)
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False, engine="pyarrow")
    tamanho_mb = buffer.tell() / (1024 * 1024)
    buffer.seek(0)

    try:
        container_client = client.get_container_client(AZURE_CONTAINER)
        if not container_client.exists():
            container_client.create_container()
            log.info(f"Container '{AZURE_CONTAINER}' criado no Azure")
    except Exception as e:
        log.error(f"Falha ao acessar/criar container '{AZURE_CONTAINER}': {type(e).__name__}: {e}")
        return False

    for tentativa in range(1, 4):
        try:
            container_client.upload_blob(name=blob_name, data=buffer, overwrite=True)
            log.info(f"Upload OK → container '{AZURE_CONTAINER}' / blob '{blob_name}' "
                     f"({len(df):,} registros, {tamanho_mb:.2f} MB)")
            return True
        except Exception as e:
            if tentativa == 3:
                log.error(f"Falha no upload para o Azure ({nome_arquivo}) após 3 tentativas: "
                          f"{type(e).__name__}: {e}")
                return False
            log.warning(f"Upload falhou (tentativa {tentativa}/3) — aguardando 30s... ({e})")
            buffer.seek(0)
            time.sleep(30)
    return False


# ------------------------------------------------------------------
# Download e parsing
# ------------------------------------------------------------------
def _urls_tce(anos: list) -> list:
    urls = []
    for ano in anos:
        for mes in range(1, 13):
            urls.append((
                f"{ano}-{mes:02d}",
                f"{TCE_BASE}/licitacao-{ano}-{mes:02d}_0.zip",
            ))
    return urls


def baixar_mes_tce(periodo: str, url: str) -> pd.DataFrame:
    """Baixa e filtra o zip mensal do TCE. Tenta 3 vezes em caso de falha de
    conexão/timeout, com espera crescente — mesmo espírito do retry de
    conexão do pncp.py."""
    resp = None
    for tentativa in range(1, 4):
        try:
            resp = requests.get(url, timeout=120)
            resp.raise_for_status()
            break
        except Exception as e:
            if tentativa == 3:
                log.error(f"{periodo}: {type(e).__name__} após 3 tentativas — desistindo")
                return pd.DataFrame()
            espera = 10 * tentativa
            log.warning(f"{periodo}: {type(e).__name__} — aguardando {espera}s (tentativa {tentativa}/3)")
            time.sleep(espera)

    CHUNK_SIZE = 50_000  # linhas por pedaço — filtra e descarta o resto sem
                          # nunca ter o CSV mensal inteiro (todas as
                          # licitações, não só obras) inteiro na memória

    def _ler_filtrando(fonte, compression=None) -> pd.DataFrame:
        pedacos = []
        for chunk in pd.read_csv(fonte, sep=";", encoding="latin-1", on_bad_lines="skip",
                                  dtype=str, chunksize=CHUNK_SIZE, compression=compression):
            filtrado = chunk[chunk["Objeto"] == "Obras e servicos de engenharia"]
            if not filtrado.empty:
                pedacos.append(filtrado.copy())
            del chunk
        return pd.concat(pedacos, ignore_index=True) if pedacos else pd.DataFrame()

    try:
        with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
            with zf.open(zf.namelist()[0]) as f:
                df = _ler_filtrando(f)
    except NotImplementedError:
        log.warning(f"{periodo}: compressão não suportada — tentando fallback...")
        tmp = tempfile.mktemp(suffix=".zip")
        try:
            with open(tmp, "wb") as f:
                f.write(resp.content)
            df = _ler_filtrando(tmp, compression="zip")
        except Exception as e2:
            log.error(f"{periodo}: fallback também falhou — {type(e2).__name__}: {e2}")
            return pd.DataFrame()
        finally:
            if os.path.exists(tmp):
                os.remove(tmp)

    resp = None  # libera o conteúdo bruto do zip (pode ter dezenas de MB) assim que possível
    df["periodo"] = periodo
    df["dt_ingestao"] = str(date.today())

    log.info(f"{periodo}: {len(df):,} linhas de obras")
    return df


# ------------------------------------------------------------------
# Bronze — mesmo padrão do ingest_pncp_bronze: baixa existente, mescla em
# memória, sobe por lote, checkpoint só após upload confirmado
# ------------------------------------------------------------------
def ingest_tce_bronze(anos: list, modo: str, lote_meses: int = 6, skip_upload: bool = False) -> pd.DataFrame:
    """
    modo:
      - 'overwrite': ignora qualquer bronze existente no Azure e recomeça
                     do zero (limpa o checkpoint local também)
      - 'append'   : baixa o bronze existente do Azure (memória, sem disco),
                     mantém o checkpoint local, só busca períodos pendentes

    GARANTIA: um período só entra no checkpoint DEPOIS que o upload do lote
    correspondente é confirmado no Azure — nunca antes. skip_upload faz o
    checkpoint nunca avançar (correto, só menos eficiente).

    Retorna o DataFrame bronze completo (prévio + coletado nesta execução).
    """
    if modo == "overwrite" and CHECKPOINT_PATH.exists():
        CHECKPOINT_PATH.unlink()

    checkpoint = carregar_checkpoint()
    log.info(f"Checkpoint: {len(checkpoint):,} período(s) já concluído(s) em execuções anteriores")

    if skip_upload:
        log.info("--skip-upload ativo: nada será enviado ao Azure, o checkpoint não avança nesta "
                  "execução. Seguro, só menos eficiente — tudo será refeito na próxima execução real.")

    if modo == "overwrite" or not _azure_configurado():
        bronze_df = pd.DataFrame()
    else:
        log.info("Baixando bronze existente do Azure (memória, sem disco)...")
        bronze_df = baixar_parquet_do_blob(NOME_BRONZE, "bronze")

    urls_todas = _urls_tce(anos)
    urls_pendentes = [(p, u) for p, u in urls_todas if p not in checkpoint]
    puladas = len(urls_todas) - len(urls_pendentes)
    log.info(f"{len(urls_pendentes)} período(s) a processar"
             + (f" ({puladas} já concluído(s) via checkpoint)" if puladas else ""))

    lote = []
    periodos_lote = []
    total_novo = 0
    falhos = []

    def _fechar_lote():
        """Sobe o lote acumulado (mesclado com o bronze existente) para o
        Azure. Só confirma no checkpoint os períodos deste lote se o upload
        for bem-sucedido."""
        nonlocal bronze_df
        if not lote:
            return
        df_lote = pd.concat(lote, ignore_index=True)
        df_lote.columns = COLUNAS
        combinado = pd.concat([bronze_df, df_lote], ignore_index=True) if not bronze_df.empty else df_lote

        if skip_upload:
            log.warning(f"--skip-upload ativo: {len(periodos_lote)} período(s) "
                        f"({periodos_lote[0]}..{periodos_lote[-1]}) NÃO entram no checkpoint.")
            return

        log.info(f"Subindo bronze para o Azure (lote: {periodos_lote[0]}..{periodos_lote[-1]}, "
                 f"{len(df_lote):,} linhas novas)...")
        sucesso = upload_dataframe_parquet(combinado, NOME_BRONZE, subpasta="bronze")
        if sucesso:
            bronze_df = combinado
            checkpoint.update(periodos_lote)
            salvar_checkpoint(checkpoint)
        else:
            log.warning(f"Upload falhou — {len(periodos_lote)} período(s) deste lote NÃO entram "
                        f"no checkpoint e serão refeitos na próxima execução.")

    for i, (periodo, url) in enumerate(urls_pendentes):
        try:
            df = baixar_mes_tce(periodo, url)
        except Exception as e:
            log.error(f"{periodo}: exceção não tratada — {type(e).__name__}: {e}")
            traceback.print_exc()
            falhos.append(periodo)
            continue

        if df.empty:
            falhos.append(periodo)
        else:
            lote.append(df)
            periodos_lote.append(periodo)
            total_novo += len(df)

        ultimo = (i == len(urls_pendentes) - 1)
        if len(lote) >= lote_meses or (ultimo and lote):
            _fechar_lote()
            lote = []
            periodos_lote = []

        time.sleep(DELAY_ENTRE_DOWNLOADS)

    log.info(f"Total coletado nesta execução: {total_novo:,} linhas novas "
             f"(total acumulado no bronze: {len(bronze_df):,})")
    if falhos:
        log.warning(f"Período(s) sem dado ou com falha de download ({len(falhos)}), "
                    f"serão retentados no próximo append: {falhos}")

    return bronze_df


# ------------------------------------------------------------------
# Silver — sempre reprocessa o bronze inteiro do zero (mesmo padrão do
# transform_silver_contratacoes do pncp.py, sem checkpoint próprio)
# ------------------------------------------------------------------
def transformar_silver(df: pd.DataFrame) -> pd.DataFrame:
    """Regras de limpeza/tipagem da bronze -> silver. Ajuste conforme as
    regras de negócio reais — cobre tipos numéricos e datas no formato
    brasileiro, texto sem espaços nas pontas, e deduplicação."""
    df = df.copy()

    colunas_valor = ["vl_unit_orcamento_lote", "vl_unit_orcamento_item", "vl_proposta"]
    for col in colunas_valor:
        if col in df.columns:
            serie = (
                df[col].astype(str)
                .str.replace(".", "", regex=False)
                .str.replace(",", ".", regex=False)
            )
            df[col] = pd.to_numeric(serie, errors="coerce")

    colunas_qtd = ["qtd_contratada", "qtd_orcamento_lote", "qtd_orcamento_item"]
    for col in colunas_qtd:
        if col in df.columns:
            serie = df[col].astype(str).str.replace(",", ".", regex=False)
            df[col] = pd.to_numeric(serie, errors="coerce")

    if "dt_edital" in df.columns:
        df["dt_edital"] = pd.to_datetime(df["dt_edital"], errors="coerce", dayfirst=True)
    if "dt_ingestao" in df.columns:
        df["dt_ingestao"] = pd.to_datetime(df["dt_ingestao"], errors="coerce")

    colunas_texto = [c for c in df.columns if pd.api.types.is_string_dtype(df[c])]
    df[colunas_texto] = df[colunas_texto].apply(lambda s: s.str.strip())

    df = df.drop_duplicates()
    return df


def ingest_silver(bronze_df: pd.DataFrame = None, skip_upload: bool = False) -> pd.DataFrame:
    """Regenera a silver por completo a partir do bronze corrente. Se
    bronze_df não for passado (ex: --skip-ingest), baixa o bronze do Azure
    para a memória. Nunca lê/grava nada em disco."""
    if bronze_df is None:
        log.info("Baixando bronze do Azure (memória, sem disco)...")
        bronze_df = baixar_parquet_do_blob(NOME_BRONZE, "bronze")

    if bronze_df.empty:
        raise ValueError("Bronze está vazio (nem em memória, nem no Azure). Rode a ingestão primeiro "
                          "(sem --skip-ingest) ou confira se o .env do Azure está configurado corretamente.")

    silver = transformar_silver(bronze_df)
    log.info(f"{len(silver):,} registros silver gerados em memória")

    if not skip_upload:
        upload_dataframe_parquet(silver, NOME_SILVER, subpasta="silver")

    return silver


# ------------------------------------------------------------------
# CLI / main — mesmo padrão do pncp.py
# ------------------------------------------------------------------
def parse_args():
    parser = argparse.ArgumentParser(description="Ingestão TCE-SP AUDESP (obras) → parquet no Azure (lake)")
    parser.add_argument("--anos", nargs="+", type=int, default=[2022, 2023, 2024],
                         help="Anos a coletar, ex: --anos 2022 2023 2024")
    parser.add_argument("--modo", choices=["append", "overwrite"], default="append",
                         help="append: mescla com o bronze/silver existentes no Azure | overwrite: recomeça do zero")
    parser.add_argument("--skip-ingest", action="store_true",
                         help="Pula a ingestão e só reprocessa o bronze existente (baixado do Azure) para silver")
    parser.add_argument("--lote-meses", type=int, default=6,
                         help="Quantos meses acumular em memória antes de subir um lote para o Azure (padrão: 6)")
    parser.add_argument("--skip-upload", action="store_true",
                         help="Não sobe bronze/silver para o Azure. O checkpoint não avança nesta execução — "
                              "use só para testes pontuais.")
    return parser.parse_args()


def main():
    args = parse_args()

    log.info(f"Anos: {args.anos}")
    log.info(f"Modo: {args.modo}")
    log.info(f"Lote: {args.lote_meses} mês(es) por upload")
    log.info(f"Upload Azure: {'desabilitado (--skip-upload)' if args.skip_upload else ('configurado' if _azure_configurado() else 'sem credenciais no .env')}")

    bronze_df = None
    if not args.skip_ingest:
        log.info("=== Bronze TCE-SP AUDESP ===")
        bronze_df = ingest_tce_bronze(args.anos, args.modo, args.lote_meses, skip_upload=args.skip_upload)

    log.info("=== Silver TCE-SP AUDESP ===")
    ingest_silver(bronze_df=bronze_df, skip_upload=args.skip_upload)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log.warning("Interrompido manualmente (Ctrl+C). O checkpoint local só marca um período como "
                    "concluído DEPOIS que o upload daquele lote é confirmado no Azure — então nenhum "
                    "dado 'sumido' vai aparecer como já processado. O que não deu tempo de subir "
                    "simplesmente não entrou no checkpoint e será refeito automaticamente na próxima "
                    "execução em --modo append.")
    except Exception:
        log.error("O script encerrou por causa de um erro não previsto:")
        traceback.print_exc()
        raise