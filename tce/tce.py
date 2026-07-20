"""
Ingestão bronze/silver — TCE-SP AUDESP (licitações/contratos de obras)
Versão Python puro (sem PySpark/Databricks), para rodar em VM Linux via cron.

Saída: bronze fica particionada por período (Parquet estilo Hive); silver
é consolidada num único arquivo parquet com todos os períodos empilhados:
    <container>/tce/bronze/periodo=YYYY-MM/part-<uuid>.parquet
    <container>/tce/silver/tce_licitacoes_obras_consolidado.parquet

Checkpoint local (JSON): guarda os períodos já processados com sucesso em
CHECKPOINT_BRONZE_PATH / CHECKPOINT_SILVER_PATH. Controlado por
CHECKPOINT_APPEND=true|false:
    true  -> retoma do checkpoint, pulando períodos já concluídos (padrão)
    false -> ignora checkpoint existente e reprocessa tudo do zero

Etapas controladas via linha de comando (não mais por variável de ambiente):
    python tce.py            -> roda bronze + silver (padrão)
    python tce.py --bronze   -> roda só a bronze
    python tce.py --silver   -> roda só a silver (a partir da bronze já existente)

.env fica na raiz do projeto (um nível acima da pasta deste script), não
versionar:
    AZURE_STORAGE_CONNECTION_STRING=...
    ou
    AZURE_STORAGE_ACCOUNT_NAME=...
    AZURE_STORAGE_ACCOUNT_KEY=...
    AZURE_STORAGE_CONTAINER=conteiner
    AZURE_BLOB_PREFIX_tce=tce
"""

import argparse
import io
import os
import json
import time
import uuid
import zipfile
import tempfile
import logging
import traceback
from datetime import date, datetime

import requests
import pandas as pd
import zipfile_deflate64  # noqa: F401  (habilita suporte a deflate64 no zipfile)
from pathlib import Path
from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient

# ------------------------------------------------------------------
# Config
# ------------------------------------------------------------------
# .env fica na raiz do projeto (um nível acima da pasta do script), igual ao
# padrão usado no pncp.py — evita duplicar credenciais por pipeline.
BASE_DIR = os.path.dirname(os.path.abspath(__file__))  # .../tce/

load_dotenv(os.path.join(BASE_DIR, "..", ".env"), override=True)  # lê o .env da raiz do projeto

TCE_BASE = "https://transparencia.tce.sp.gov.br/sites/default/files/conjunto-dados/licitacoes-contratos"

ENV_PATH = os.path.normpath(os.path.join(BASE_DIR, "..", ".env"))

# ── Config Azure Blob Storage (tudo vem do .env, nunca hardcoded) ─────────
# Opção A (mais simples): AZURE_STORAGE_CONNECTION_STRING sozinha
# Opção B: AZURE_STORAGE_ACCOUNT_NAME + AZURE_STORAGE_ACCOUNT_KEY
# Em ambos os casos, AZURE_STORAGE_CONTAINER define o container de destino.
AZURE_CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
AZURE_ACCOUNT_NAME = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
AZURE_ACCOUNT_KEY = os.getenv("AZURE_STORAGE_ACCOUNT_KEY")
CONTAINER_NAME = os.getenv("AZURE_STORAGE_CONTAINER", "conteiner")
BLOB_PREFIX = os.getenv("AZURE_BLOB_PREFIX_tce", "tce").strip("/")

# Prefixo final grava a camada explicitamente no caminho, independente do nome
# do container — deixa claro no path se é dado bronze ou silver mesmo que os
# dois acabem apontando pro mesmo container por engano:
#   <container>/tce/bronze/periodo=YYYY-MM/...
#   <container>/tce/silver/periodo=YYYY-MM/...
BRONZE_PREFIX = f"{BLOB_PREFIX}/bronze"
SILVER_PREFIX = f"{BLOB_PREFIX}/silver"
# Silver agora é um único arquivo consolidado, não particionado por período
SILVER_BLOB_NAME = f"{SILVER_PREFIX}/tce_licitacoes_obras_consolidado.parquet"

ANOS = [int(a) for a in os.getenv("ANOS", "2022,2023,2024").split(",")]
LOTE_MESES = int(os.getenv("LOTE_MESES", "6"))
MODO_ESCRITA = os.getenv("MODO_ESCRITA", "append")  # "overwrite" | "append"
FONTE = os.getenv("FONTE", "tce_audesp")  # "tce_audesp" | "pncp" | "ambas"

# Checkpoint local: guarda os períodos já processados com sucesso, para retomar
# a execução sem reprocessar tudo em caso de queda/reinício.
CHECKPOINT_DIR = Path(__file__).resolve().parent
CHECKPOINT_BRONZE_PATH = Path(os.getenv("CHECKPOINT_BRONZE_PATH", str(CHECKPOINT_DIR / "checkpoint_bronze.json")))
CHECKPOINT_SILVER_PATH = Path(os.getenv("CHECKPOINT_SILVER_PATH", str(CHECKPOINT_DIR / "checkpoint_silver.json")))
# CHECKPOINT_APPEND=true  -> retoma do checkpoint existente, pulando períodos já concluídos
# CHECKPOINT_APPEND=false -> ignora checkpoint existente e reprocessa tudo do zero
CHECKPOINT_APPEND = os.getenv("CHECKPOINT_APPEND", "true").strip().lower() in ("1", "true", "yes")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger("bronze_tce")

# O SDK do Azure (azure-core/azure-storage-blob) loga cada request/response HTTP
# em nível INFO, o que polui o log. Sobe o nível só pros loggers dele.
for _azure_logger in ("azure", "azure.core.pipeline.policies.http_logging_policy"):
    logging.getLogger(_azure_logger).setLevel(logging.WARNING)

log.info(f".env carregado de: {ENV_PATH} (existe: {os.path.exists(ENV_PATH)})")
log.info(f"CONTAINER_NAME efetivo: {CONTAINER_NAME}")
log.info(f"bronze path: {BRONZE_PREFIX} | silver (consolidado): {SILVER_BLOB_NAME}")
log.info(
    "Conta: "
    + (AZURE_ACCOUNT_NAME or "(via connection string)" if AZURE_CONNECTION_STRING or AZURE_ACCOUNT_NAME else "NENHUMA CREDENCIAL")
)

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
# Checkpoint local
# ------------------------------------------------------------------
def carregar_checkpoint(path: Path) -> set:
    """Lê o checkpoint local. Retorna conjunto vazio se não existir ou estiver corrompido."""
    if not path.exists():
        return set()
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return set(data.get("periodos_concluidos", []))
    except Exception as e:
        log.warning(f"Checkpoint '{path}' ilegível ({type(e).__name__}) — iniciando do zero")
        return set()


def salvar_checkpoint(path: Path, concluidos: set):
    """Grava o checkpoint de forma atômica (escreve em .tmp e renomeia),
    evitando corromper o arquivo se o processo for interrompido no meio da escrita."""
    tmp_path = path.with_suffix(".tmp")
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(
            {
                "periodos_concluidos": sorted(concluidos),
                "atualizado_em": datetime.now().isoformat(timespec="seconds"),
            },
            f,
            indent=2,
            ensure_ascii=False,
        )
    tmp_path.replace(path)  # rename é atômico no mesmo filesystem


# ------------------------------------------------------------------
# Cliente Azure Blob
# ------------------------------------------------------------------
def get_blob_service_client() -> BlobServiceClient:
    if AZURE_CONNECTION_STRING:
        return BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)
    if AZURE_ACCOUNT_NAME and AZURE_ACCOUNT_KEY:
        account_url = f"https://{AZURE_ACCOUNT_NAME}.blob.core.windows.net"
        return BlobServiceClient(account_url=account_url, credential=AZURE_ACCOUNT_KEY)
    raise RuntimeError(
        "Nenhuma credencial Azure encontrada.\n"
        f"  .env procurado em: {ENV_PATH} (existe: {os.path.exists(ENV_PATH)})\n"
        f"  AZURE_STORAGE_CONNECTION_STRING lida: {'sim' if AZURE_CONNECTION_STRING else 'não'}\n"
        f"  AZURE_STORAGE_ACCOUNT_NAME lida: {'sim' if AZURE_ACCOUNT_NAME else 'não'}\n"
        f"  AZURE_STORAGE_ACCOUNT_KEY lida: {'sim' if AZURE_ACCOUNT_KEY else 'não'}\n"
        "Configure AZURE_STORAGE_CONNECTION_STRING ou "
        "AZURE_STORAGE_ACCOUNT_NAME + AZURE_STORAGE_ACCOUNT_KEY no .env"
    )


def ensure_container(bsc: BlobServiceClient, container_name: str):
    """upload_blob não cria o container — garante que ele existe antes de subir dados."""
    container_client = bsc.get_container_client(container_name)
    if not container_client.exists():
        log.warning(f"Container '{container_name}' não existe — criando...")
        container_client.create_container()
    return container_client


def upload_parquet(
    bsc: BlobServiceClient, df: pd.DataFrame, periodo: str,
    container_name: str = CONTAINER_NAME, blob_prefix: str = BLOB_PREFIX,
) -> str:
    """Grava um DataFrame como Parquet em memória e sobe para o container,
    particionado por período (estilo Hive: periodo=YYYY-MM/)."""
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False, engine="pyarrow")
    buffer.seek(0)

    blob_path = f"{blob_prefix}/periodo={periodo}/part-{uuid.uuid4().hex}.parquet"
    container_client = bsc.get_container_client(container_name)

    for tentativa in range(1, 4):
        try:
            container_client.upload_blob(name=blob_path, data=buffer, overwrite=True)
            return blob_path
        except Exception as e:
            if tentativa == 3:
                raise
            log.warning(f"Upload falhou (tent. {tentativa}/3) — aguardando 30s... ({e})")
            buffer.seek(0)
            time.sleep(30)


def limpar_particao(
    bsc: BlobServiceClient, periodo: str,
    container_name: str = CONTAINER_NAME, blob_prefix: str = BLOB_PREFIX,
):
    """Equivalente a modo='overwrite' no Delta: apaga blobs existentes da partição
    antes de escrever, para não duplicar dados em reprocessamentos."""
    container_client = bsc.get_container_client(container_name)
    prefixo = f"{blob_prefix}/periodo={periodo}/"
    for blob in container_client.list_blobs(name_starts_with=prefixo):
        container_client.delete_blob(blob.name)


def listar_periodos(
    bsc: BlobServiceClient, container_name: str, blob_prefix: str,
) -> list:
    """Lista os períodos (partições) já existentes em um container/prefixo."""
    container_client = bsc.get_container_client(container_name)
    prefixo = f"{blob_prefix}/periodo="
    periodos = set()
    for blob in container_client.list_blobs(name_starts_with=prefixo):
        resto = blob.name[len(prefixo):]
        periodos.add(resto.split("/")[0])
    return sorted(periodos)


def ler_particao(
    bsc: BlobServiceClient, periodo: str,
    container_name: str = CONTAINER_NAME, blob_prefix: str = BLOB_PREFIX,
) -> pd.DataFrame:
    """Lê todos os arquivos parquet de uma partição (periodo=) e concatena."""
    container_client = bsc.get_container_client(container_name)
    prefixo = f"{blob_prefix}/periodo={periodo}/"
    frames = []
    for blob in container_client.list_blobs(name_starts_with=prefixo):
        conteudo = container_client.get_blob_client(blob.name).download_blob().readall()
        frames.append(pd.read_parquet(io.BytesIO(conteudo)))
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


# ------------------------------------------------------------------
# Download e parsing (equivalente ao original)
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
    try:
        resp = requests.get(url, timeout=120)
        resp.raise_for_status()
    except Exception as e:
        log.error(f"{periodo}: {type(e).__name__}")
        return pd.DataFrame()

    try:
        with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
            with zf.open(zf.namelist()[0]) as f:
                df = pd.read_csv(
                    f, sep=";", encoding="latin-1",
                    on_bad_lines="skip", dtype=str,
                )
    except NotImplementedError:
        log.warning(f"{periodo}: compressão não suportada — tentando fallback...")
        tmp = tempfile.mktemp(suffix=".zip")
        try:
            with open(tmp, "wb") as f:
                f.write(resp.content)
            df = pd.read_csv(
                tmp, sep=";", encoding="latin-1",
                on_bad_lines="skip", dtype=str, compression="zip",
            )
        except Exception as e2:
            log.error(f"{periodo}: fallback também falhou — {type(e2).__name__}: {e2}")
            return pd.DataFrame()
        finally:
            if os.path.exists(tmp):
                os.remove(tmp)

    df = df[df["Objeto"] == "Obras e servicos de engenharia"].copy()
    df["periodo"] = periodo
    df["dt_ingestao"] = str(date.today())

    log.info(f"{periodo}: {len(df):,} linhas de obras")
    return df


# ------------------------------------------------------------------
# Orquestração
# ------------------------------------------------------------------
def ingest_tce_bronze(
    anos: list, modo: str, lote_meses: int = 6,
    checkpoint_append: bool = CHECKPOINT_APPEND,
):
    bsc = get_blob_service_client()
    ensure_container(bsc, CONTAINER_NAME)

    concluidos = carregar_checkpoint(CHECKPOINT_BRONZE_PATH) if checkpoint_append else set()
    if concluidos:
        log.info(f"Checkpoint bronze: {len(concluidos)} períodos já concluídos — pulando")

    urls_todas = _urls_tce(anos)
    urls = [(p, u) for p, u in urls_todas if p not in concluidos]
    if len(urls) < len(urls_todas):
        log.info(f"{len(urls_todas) - len(urls)} períodos pulados via checkpoint, {len(urls)} pendentes")

    total = 0
    falhos = []
    lote = []
    periodos_gravados = set()

    def _salvar_lote(lote: list) -> int:
        if not lote:
            return 0
        df_lote = pd.concat(lote, ignore_index=True)
        df_lote.columns = COLUNAS
        n = len(df_lote)

        # Agrupa por período para gravar cada partição isoladamente
        for periodo, grupo in df_lote.groupby("periodo"):
            if modo == "overwrite" and periodo not in periodos_gravados:
                limpar_particao(bsc, periodo, CONTAINER_NAME, BRONZE_PREFIX)
            blob_path = upload_parquet(bsc, grupo, periodo, CONTAINER_NAME, BRONZE_PREFIX)
            log.info(f"  → {blob_path} ({len(grupo):,} linhas)")
            periodos_gravados.add(periodo)

            # Checkpoint salvo por período, não só por lote: se o processo cair
            # no meio, o progresso já feito não se perde.
            concluidos.add(periodo)
            salvar_checkpoint(CHECKPOINT_BRONZE_PATH, concluidos)

        del df_lote
        return n

    for i, (periodo, url) in enumerate(urls):
        try:
            df = baixar_mes_tce(periodo, url)
            if not df.empty:
                lote.append(df)
        except Exception as e:
            log.error(f"{periodo}: {type(e).__name__} — pulando")
            falhos.append(periodo)
            continue

        ultimo = (i == len(urls) - 1)
        if len(lote) >= lote_meses or (ultimo and lote):
            try:
                n = _salvar_lote(lote)
                total += n
                lote = []
                log.info(f"Lote salvo — {n:,} linhas | total acumulado: {total:,}")
            except Exception as e:
                log.error(f"Falha ao salvar lote: {type(e).__name__}: {e}")
                falhos.extend([p for p, _ in urls[max(0, i - lote_meses + 1):i + 1]])
                lote = []

    log.info(f"Total salvo: {total:,} linhas → {CONTAINER_NAME}/{BRONZE_PREFIX}")
    if falhos:
        log.warning(f"Períodos com falha ({len(falhos)}) — rode novamente com MODO_ESCRITA=append:")
        log.warning(f"{falhos}")


# ------------------------------------------------------------------
# Silver
# ------------------------------------------------------------------
def transformar_silver(df: pd.DataFrame) -> pd.DataFrame:
    """Regras de limpeza/tipagem da bronze -> silver. Ajuste conforme as
    regras de negócio reais — isso cobre o básico (tipos numéricos e datas
    no formato brasileiro, texto sem espaços nas pontas, deduplicação)."""
    df = df.copy()

    colunas_valor = ["vl_unit_orcamento_lote", "vl_unit_orcamento_item", "vl_proposta"]
    for col in colunas_valor:
        if col in df.columns:
            serie = (
                df[col].astype(str)
                .str.replace(".", "", regex=False)   # remove separador de milhar
                .str.replace(",", ".", regex=False)  # normaliza separador decimal
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


def ler_parquet_unico(bsc: BlobServiceClient, container_name: str, blob_name: str) -> pd.DataFrame:
    """Lê um único blob parquet. Retorna DataFrame vazio se o blob ainda não existir."""
    blob_client = bsc.get_container_client(container_name).get_blob_client(blob_name)
    if not blob_client.exists():
        return pd.DataFrame()
    conteudo = blob_client.download_blob().readall()
    return pd.read_parquet(io.BytesIO(conteudo))


def upload_parquet_unico(bsc: BlobServiceClient, df: pd.DataFrame, container_name: str, blob_name: str):
    """Grava um DataFrame como um único arquivo parquet, sobrescrevendo o existente."""
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False, engine="pyarrow")
    buffer.seek(0)
    container_client = bsc.get_container_client(container_name)

    for tentativa in range(1, 4):
        try:
            container_client.upload_blob(name=blob_name, data=buffer, overwrite=True)
            return
        except Exception as e:
            if tentativa == 3:
                raise
            log.warning(f"Upload do consolidado falhou (tent. {tentativa}/3) — aguardando 30s... ({e})")
            buffer.seek(0)
            time.sleep(30)


def ingest_silver(checkpoint_append: bool = CHECKPOINT_APPEND):
    """Lê as partições pendentes da bronze, aplica transformar_silver e empilha
    tudo (dados novos + consolidado já existente, se houver) em um único
    arquivo parquet na silver — em vez de uma partição por período.
    Usa checkpoint próprio (CHECKPOINT_SILVER_PATH), independente do checkpoint
    da bronze, pra saber quais períodos já foram incorporados ao consolidado."""
    bsc = get_blob_service_client()
    ensure_container(bsc, CONTAINER_NAME)

    concluidos = carregar_checkpoint(CHECKPOINT_SILVER_PATH) if checkpoint_append else set()
    if concluidos:
        log.info(f"Checkpoint silver: {len(concluidos)} períodos já incorporados ao consolidado")

    periodos_bronze = listar_periodos(bsc, CONTAINER_NAME, BRONZE_PREFIX)
    pendentes = [p for p in periodos_bronze if p not in concluidos]

    if not pendentes:
        log.info("Nenhum período pendente para a camada silver.")
        return

    log.info(f"{len(pendentes)} período(s) pendente(s) para silver: {pendentes}")

    novos_frames = []
    falhos = []

    for periodo in pendentes:
        try:
            df_bronze = ler_particao(bsc, periodo, CONTAINER_NAME, BRONZE_PREFIX)
            if df_bronze.empty:
                log.warning(f"{periodo}: nenhuma linha na bronze — pulando")
                continue

            df_silver = transformar_silver(df_bronze)
            novos_frames.append(df_silver)
            log.info(f"{periodo}: {len(df_silver):,} linhas transformadas")

        except Exception as e:
            log.error(f"{periodo}: falha ao transformar — {type(e).__name__}: {e}")
            falhos.append(periodo)

    if not novos_frames:
        log.warning("Nenhum dado novo transformado com sucesso — nada a gravar na silver.")
        return

    df_novo = pd.concat(novos_frames, ignore_index=True)

    # Junta com o consolidado já existente (se houver) e deduplica, pra rodar
    # de novo não gerar linhas repetidas.
    df_existente = ler_parquet_unico(bsc, CONTAINER_NAME, SILVER_BLOB_NAME)
    if not df_existente.empty:
        df_final = pd.concat([df_existente, df_novo], ignore_index=True).drop_duplicates()
    else:
        df_final = df_novo.drop_duplicates()

    upload_parquet_unico(bsc, df_final, CONTAINER_NAME, SILVER_BLOB_NAME)
    log.info(f"Consolidado silver salvo — {len(df_final):,} linhas totais → {CONTAINER_NAME}/{SILVER_BLOB_NAME}")

    processados = [p for p in pendentes if p not in falhos]
    concluidos.update(processados)
    salvar_checkpoint(CHECKPOINT_SILVER_PATH, concluidos)

    if falhos:
        log.warning(f"Períodos com falha na silver ({len(falhos)}): {falhos}")


def parse_args():
    parser = argparse.ArgumentParser(
        description="Ingestão bronze/silver TCE-SP AUDESP. Sem flags, roda as duas etapas."
    )
    parser.add_argument(
        "--bronze", action="store_true",
        help="Executa somente a etapa bronze (ingestão a partir da fonte TCE)",
    )
    parser.add_argument(
        "--silver", action="store_true",
        help="Executa somente a etapa silver (transformação a partir da bronze já existente)",
    )
    args = parser.parse_args()

    if args.bronze and args.silver:
        return "ambas"
    if args.bronze:
        return "bronze"
    if args.silver:
        return "silver"
    return "ambas"  # padrão: nenhuma flag informada -> roda as duas etapas


if __name__ == "__main__":
    etapa = parse_args()
    try:
        if FONTE in ("tce_audesp", "ambas"):
            if etapa in ("bronze", "ambas"):
                log.info("=== Bronze TCE-SP AUDESP ===")
                ingest_tce_bronze(ANOS, MODO_ESCRITA, LOTE_MESES)

            if etapa in ("silver", "ambas"):
                log.info("=== Silver TCE-SP AUDESP ===")
                ingest_silver()
    except KeyboardInterrupt:
        log.warning(
            "Interrompido manualmente (Ctrl+C). O progresso já salvo no checkpoint "
            "não foi perdido — rode de novo (CHECKPOINT_APPEND=true) para continuar de onde parou."
        )
    except Exception:
        log.error("O script encerrou por causa de um erro não previsto:")
        traceback.print_exc()
        raise