"""
seade.py

Download e tratamento das bases de investimentos do SEADE (SP). Adaptado de
notebook Databricks/Spark (Unity Catalog, Volumes) para Python puro,
rodando em Linux, no mesmo padrão arquitetural do pncp.py/tce.py.

Cada fonte é um dicionário em FONTES (nome de destino, URL, delimitador,
encoding). Para cada fonte, o script:
  - baixa o arquivo (CSV, XLSX, ou ZIP/7z contendo CSV/XLSX) direto para a
    memória — nunca grava em disco, exceto scratch temporário e transitório
    necessário para extrair .7z (apagado imediatamente depois);
  - sobe o(s) arquivo(s) tabular(es) crus para o Azure — camada bronze;
  - lê, normaliza nomes de coluna (sem acento/espaço/maiúscula) e sobe como
    Parquet — camada silver.

Sem cópia local persistente de dado nenhum — só um checkpoint pequeno marca
quais fontes já foram processadas com sucesso (bronze + silver confirmados
no Azure). GARANTIA: uma fonte só entra no checkpoint depois que os dois
uploads são confirmados — nunca antes.

Saídas:
  {AZURE_STORAGE_CONTAINER}/{AZURE_BLOB_PREFIX}/bronze/{fonte}/{arquivo_original}
  {AZURE_STORAGE_CONTAINER}/{AZURE_BLOB_PREFIX}/silver/{fonte}.parquet

Uso:
  python seade.py                                    # processa todas as fontes cadastradas
  python seade.py --fontes investimentos_captados     # só uma fonte específica
  python seade.py --modo overwrite                    # ignora checkpoint, reprocessa tudo
  python seade.py --skip-download                     # reprocessa a silver a partir do bronze já no Azure
  python seade.py --skip-upload                       # teste pontual — checkpoint não avança

.env na raiz do projeto (um nível acima da pasta deste script), não versionar:
  AZURE_STORAGE_CONNECTION_STRING=...
  ou
  AZURE_STORAGE_ACCOUNT_NAME=...
  AZURE_STORAGE_ACCOUNT_KEY=...
  AZURE_STORAGE_CONTAINER=conteiner
  AZURE_BLOB_PREFIX=seade
"""

import argparse
import io
import json
import logging
import os
import re
import tempfile
import time
import traceback
import zipfile
from datetime import datetime
from pathlib import Path
from urllib.parse import unquote, urlparse

import pandas as pd
import requests
from dotenv import load_dotenv
from tqdm import tqdm
from unidecode import unidecode

try:
    from azure.storage.blob import BlobServiceClient
except ImportError:
    BlobServiceClient = None

try:
    import py7zr
except ImportError:
    py7zr = None

# ------------------------------------------------------------------
# Config
# ------------------------------------------------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))  # .../seade/
ENV_PATH = os.path.normpath(os.path.join(BASE_DIR, "..", ".env"))
load_dotenv(ENV_PATH, override=True)

HTTP_HEADERS = {"User-Agent": "Mozilla/5.0"}

# ── Config Azure Blob Storage (tudo vem do .env, nunca hardcoded) ─────────
AZURE_CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
AZURE_ACCOUNT_NAME = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
AZURE_ACCOUNT_KEY = os.getenv("AZURE_STORAGE_ACCOUNT_KEY")
AZURE_CONTAINER = os.getenv("AZURE_STORAGE_CONTAINER", "conteiner")
AZURE_BLOB_PREFIX = os.getenv("AZURE_BLOB_PREFIX_seade", "seade").strip("/")

DATA_DIR = Path(os.path.normpath(os.path.join(BASE_DIR, "..", "data")))
CHECKPOINT_PATH = Path(os.getenv("CHECKPOINT_PATH", str(DATA_DIR / "checkpoint_seade.json")))

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("seade")
for _azure_logger in ("azure", "azure.core.pipeline.policies.http_logging_policy"):
    logging.getLogger(_azure_logger).setLevel(logging.WARNING)

log.info(f".env carregado de: {ENV_PATH} (existe: {os.path.exists(ENV_PATH)})")
log.info(f"Container: {AZURE_CONTAINER} | prefixo: {AZURE_BLOB_PREFIX}")

# ── Fontes cadastradas (equivalente aos 3 blocos do notebook original) ───
FONTES = [
    {
        "nome": "investimentos_captados",
        "fonte_original": "https://repositorio.seade.gov.br/dataset/seade-investimentos/resource/3ee8eb9b-a3b5-4d53-925f-71e5dabb263c",
        "url": "https://repositorio.seade.gov.br/dataset/82fac7ed-4417-4745-b1a1-2172916aecf4/resource/3ee8eb9b-a3b5-4d53-925f-71e5dabb263c/download/piesp_captados.csv",
        "delimitador": ";",
        "encoding": "windows-1252",
    },
    {
        "nome": "investimentos_captados_com_valor",
        "fonte_original": "https://repositorio.seade.gov.br/dataset/seade-investimentos/resource/468a7394-bbac-4923-9b3f-e76b7ea51d14",
        "url": "https://repositorio.seade.gov.br/dataset/82fac7ed-4417-4745-b1a1-2172916aecf4/resource/468a7394-bbac-4923-9b3f-e76b7ea51d14/download/piesp_confirmados_com_valor.csv",
        "delimitador": ";",
        "encoding": "windows-1252",
    },
    {
        "nome": "investimentos_captados_sem_valor",
        "fonte_original": "https://repositorio.seade.gov.br/dataset/seade-investimentos/resource/71837298-949e-4d9e-99ce-a9be74ea51d7",
        "url": "https://repositorio.seade.gov.br/dataset/82fac7ed-4417-4745-b1a1-2172916aecf4/resource/71837298-949e-4d9e-99ce-a9be74ea51d7/download/piesp_confirmados_sem_valor.csv",
        "delimitador": ";",
        "encoding": "windows-1252",
    },
]


# ------------------------------------------------------------------
# Checkpoint local
# ------------------------------------------------------------------
def carregar_checkpoint() -> set:
    if not CHECKPOINT_PATH.exists():
        return set()
    try:
        with open(CHECKPOINT_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        return set(data.get("fontes_concluidas", []))
    except Exception as e:
        log.warning(f"Checkpoint '{CHECKPOINT_PATH}' ilegível ({type(e).__name__}) — iniciando do zero")
        return set()


def salvar_checkpoint(concluidas: set):
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    tmp_path = CHECKPOINT_PATH.with_suffix(".tmp")
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(
            {"fontes_concluidas": sorted(concluidas),
             "atualizado_em": datetime.now().isoformat(timespec="seconds")},
            f, indent=2, ensure_ascii=False,
        )
    tmp_path.replace(CHECKPOINT_PATH)


# ------------------------------------------------------------------
# Azure Blob Storage — helpers em memória, mesmo padrão do pncp.py/tce.py
# ------------------------------------------------------------------
def _azure_configurado() -> bool:
    return bool(AZURE_CONNECTION_STRING) or bool(AZURE_ACCOUNT_NAME and AZURE_ACCOUNT_KEY)


def get_blob_service_client():
    if BlobServiceClient is None:
        log.warning("Pacote 'azure-storage-blob' não instalado — pulando operação no Azure. "
                    "Instale com: pip install azure-storage-blob")
        return None
    if not _azure_configurado():
        log.warning(f"Nenhuma credencial do Azure encontrada no .env ({ENV_PATH}) — pulando operação.")
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


def _get_container_client(client):
    container_client = client.get_container_client(AZURE_CONTAINER)
    if not container_client.exists():
        container_client.create_container()
        log.info(f"Container '{AZURE_CONTAINER}' criado no Azure")
    return container_client


def upload_bytes_para_blob(dados: bytes, nome_arquivo: str, subpasta: str) -> bool:
    """Sobe bytes crus (arquivo bronze, como veio da fonte) para o Azure,
    sobrescrevendo. Retry de 3 tentativas com 15s de espera."""
    client = get_blob_service_client()
    if client is None:
        return False
    blob_name = _blob_name(nome_arquivo, subpasta)
    try:
        container_client = _get_container_client(client)
    except Exception as e:
        log.error(f"Falha ao acessar/criar container '{AZURE_CONTAINER}': {type(e).__name__}: {e}")
        return False

    for tentativa in range(1, 4):
        try:
            container_client.upload_blob(name=blob_name, data=dados, overwrite=True)
            log.info(f"Upload OK (bronze) → '{blob_name}' ({len(dados) / (1024 * 1024):.2f} MB)")
            return True
        except Exception as e:
            if tentativa == 3:
                log.error(f"Falha no upload de '{blob_name}' após 3 tentativas: {type(e).__name__}: {e}")
                return False
            log.warning(f"Upload de '{blob_name}' falhou (tentativa {tentativa}/3) — aguardando 15s... ({e})")
            time.sleep(15)
    return False


def baixar_bytes_do_blob(nome_arquivo: str, subpasta: str) -> bytes:
    """Baixa um blob cru direto para a memória. None se não existir/falhar."""
    client = get_blob_service_client()
    if client is None:
        return None
    blob_name = _blob_name(nome_arquivo, subpasta)
    try:
        container_client = client.get_container_client(AZURE_CONTAINER)
        blob_client = container_client.get_blob_client(blob_name)
        if not blob_client.exists():
            return None
        return blob_client.download_blob().readall()
    except Exception as e:
        log.error(f"Falha ao baixar '{blob_name}' do Azure: {type(e).__name__}: {e}")
        return None


def listar_blobs(subpasta: str) -> list:
    """Lista nomes de blob (sem o prefixo) dentro de uma subpasta."""
    client = get_blob_service_client()
    if client is None:
        return []
    prefixo = _blob_name("", subpasta)
    try:
        container_client = client.get_container_client(AZURE_CONTAINER)
        return [b.name[len(prefixo):] for b in container_client.list_blobs(name_starts_with=prefixo)]
    except Exception as e:
        log.error(f"Falha ao listar blobs em '{prefixo}': {type(e).__name__}: {e}")
        return []


def upload_dataframe_parquet(df: pd.DataFrame, nome_arquivo: str, subpasta: str = "") -> bool:
    """Serializa df como parquet num buffer em memória e sobe para o Azure."""
    client = get_blob_service_client()
    if client is None:
        return False
    blob_name = _blob_name(nome_arquivo, subpasta)
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False, engine="pyarrow")
    tamanho_mb = buffer.tell() / (1024 * 1024)
    buffer.seek(0)

    try:
        container_client = _get_container_client(client)
    except Exception as e:
        log.error(f"Falha ao acessar/criar container '{AZURE_CONTAINER}': {type(e).__name__}: {e}")
        return False

    for tentativa in range(1, 4):
        try:
            container_client.upload_blob(name=blob_name, data=buffer, overwrite=True)
            log.info(f"Upload OK (silver) → '{blob_name}' ({len(df):,} linhas, {tamanho_mb:.2f} MB)")
            return True
        except Exception as e:
            if tentativa == 3:
                log.error(f"Falha no upload de '{blob_name}' após 3 tentativas: {type(e).__name__}: {e}")
                return False
            log.warning(f"Upload de '{blob_name}' falhou (tentativa {tentativa}/3) — aguardando 15s... ({e})")
            buffer.seek(0)
            time.sleep(15)
    return False


# ------------------------------------------------------------------
# Download e parsing (equivalente às funções url_*_para_spark do original,
# trocando Spark por pandas e Volumes do Databricks por memória/scratch
# temporário)
# ------------------------------------------------------------------
def _baixar_para_memoria(url: str) -> tuple:
    """Baixa a URL inteira para a memória, com barra de progresso. Tenta 3x
    em caso de falha de conexão, com espera crescente."""
    nome_arquivo = unquote(os.path.basename(urlparse(url).path))

    for tentativa in range(1, 4):
        try:
            resp = requests.get(url, headers=HTTP_HEADERS, stream=True, allow_redirects=True, timeout=120)
            resp.raise_for_status()
            break
        except Exception as e:
            if tentativa == 3:
                raise
            espera = 10 * tentativa
            log.warning(f"{nome_arquivo}: {type(e).__name__} — aguardando {espera}s (tentativa {tentativa}/3)")
            time.sleep(espera)

    total = resp.headers.get("content-length")
    total = int(total) if total else None
    buffer = io.BytesIO()
    with tqdm(total=total, unit="B", unit_scale=True, unit_divisor=1024, desc=f"Baixando {nome_arquivo}") as pbar:
        for chunk in resp.iter_content(chunk_size=1024 * 1024):
            if chunk:
                buffer.write(chunk)
                pbar.update(len(chunk))

    return buffer.getvalue(), nome_arquivo


def _extrair_tabulares(conteudo: bytes, nome_arquivo: str) -> list:
    """Retorna lista de (nome_interno, bytes) prontos pra leitura — extraindo
    de zip/7z se necessário. CSV/XLSX direto retornam como estão."""
    ext = nome_arquivo.split(".")[-1].lower()

    if ext in ("csv", "xlsx"):
        return [(nome_arquivo, conteudo)]

    if ext == "zip":
        arquivos = []
        with zipfile.ZipFile(io.BytesIO(conteudo)) as zf:
            for membro in zf.namelist():
                if membro.lower().endswith((".csv", ".xlsx")):
                    arquivos.append((os.path.basename(membro), zf.read(membro)))
        if not arquivos:
            raise ValueError("Nenhum CSV/XLSX encontrado dentro do zip")
        return arquivos

    if ext == "7z":
        if py7zr is None:
            raise RuntimeError("Pacote 'py7zr' não instalado — necessário para extrair .7z. "
                                "Instale com: pip install py7zr")
        # extração exige arquivo real em disco (limitação da lib) — usa um
        # diretório temporário transitório, apagado ao sair do "with"
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_arquivo = os.path.join(tmpdir, nome_arquivo)
            with open(tmp_arquivo, "wb") as f:
                f.write(conteudo)
            extrai_dir = os.path.join(tmpdir, "extraido")
            os.makedirs(extrai_dir, exist_ok=True)
            try:
                with py7zr.SevenZipFile(tmp_arquivo, mode="r") as z:
                    z.extractall(extrai_dir)
            except Exception:
                # às vezes o .7z é, na verdade, um zip disfarçado — já
                # aconteceu na prática (comentário do script original)
                with zipfile.ZipFile(tmp_arquivo, "r") as zip_ref:
                    zip_ref.extractall(extrai_dir)

            arquivos = []
            for root, _, files in os.walk(extrai_dir):
                for nome in files:
                    if nome.lower().endswith((".csv", ".xlsx")):
                        with open(os.path.join(root, nome), "rb") as f:
                            arquivos.append((nome, f.read()))
        if not arquivos:
            raise ValueError("Nenhum CSV/XLSX encontrado dentro do 7z")
        return arquivos

    raise ValueError(f"Extensão de arquivo não suportada: .{ext}")


def _ler_tabular(nome_arquivo: str, conteudo: bytes, delimitador: str, encoding: str) -> pd.DataFrame:
    ext = nome_arquivo.split(".")[-1].lower()
    if ext == "csv":
        try:
            return pd.read_csv(io.BytesIO(conteudo), sep=delimitador, encoding=encoding,
                                quotechar='"', escapechar='"', engine="python", on_bad_lines="skip")
        except UnicodeDecodeError as e:
            # O charset declarado (ex: windows-1252) tem bytes indefinidos/reservados
            # que às vezes aparecem em exports de sistemas legados. Em vez de abortar
            # a fonte inteira por causa de um byte problemático, tenta de novo
            # substituindo os bytes inválidos por um caractere de substituição (�).
            log.warning(f"{nome_arquivo}: erro de encoding em '{encoding}' ({e}) — "
                        f"tentando de novo com encoding_errors='replace' (bytes inválidos viram '�')")
            return pd.read_csv(io.BytesIO(conteudo), sep=delimitador, encoding=encoding,
                                encoding_errors="replace", quotechar='"', escapechar='"',
                                engine="python", on_bad_lines="skip")
    if ext == "xlsx":
        return pd.read_excel(io.BytesIO(conteudo), engine="openpyxl")
    raise ValueError(f"Extensão não suportada para leitura: .{ext}")


def _normalizar_coluna(col) -> str:
    col = unidecode(str(col)).strip().replace(" ", "_").replace("$", "s").lower()
    return re.sub(r"[^a-z0-9_]", "", col)


# ------------------------------------------------------------------
# Orquestração por fonte — bronze (arquivo cru) + silver (parquet tratado)
# ------------------------------------------------------------------
def processar_fonte_bronze(fonte: dict) -> list:
    """Baixa a fonte da internet, extrai se necessário, e sobe o(s)
    arquivo(s) tabular(es) crus para a camada bronze. Retorna lista de
    (nome_interno, bytes) — os mesmos dados, já em memória, prontos para a
    etapa silver (evita baixar duas vezes na mesma execução)."""
    nome = fonte["nome"]
    conteudo, nome_arquivo = _baixar_para_memoria(fonte["url"])
    tabulares = _extrair_tabulares(conteudo, nome_arquivo)

    for nome_interno, dados in tabulares:
        upload_bytes_para_blob(dados, nome_interno, subpasta=f"bronze/{nome}")

    return tabulares


def processar_fonte_silver(fonte: dict, tabulares: list) -> pd.DataFrame:
    """Lê e normaliza o(s) arquivo(s) tabular(es) da fonte (já em memória, ou
    baixados do bronze no Azure) e sobe como parquet único na silver."""
    frames = []
    for nome_interno, dados in tabulares:
        try:
            df = _ler_tabular(nome_interno, dados, fonte.get("delimitador", ";"), fonte.get("encoding", "utf-8"))
            frames.append(df)
        except Exception as e:
            log.error(f"{fonte['nome']}: falha ao ler '{nome_interno}' — {type(e).__name__}: {e}")

    if not frames:
        raise ValueError(f"{fonte['nome']}: nenhum arquivo pôde ser lido — silver não gerada")

    df_final = pd.concat(frames, ignore_index=True) if len(frames) > 1 else frames[0]
    df_final.columns = [_normalizar_coluna(c) for c in df_final.columns]
    log.info(f"{fonte['nome']}: {len(df_final):,} linhas, {len(df_final.columns)} colunas normalizadas")

    return df_final


def processar_fonte(fonte: dict, skip_download: bool, skip_upload: bool) -> bool:
    """
    Processa uma fonte de ponta a ponta (bronze + silver). Retorna True só
    se AMBAS as camadas foram confirmadas no Azure — é essa confirmação que
    autoriza a fonte a entrar no checkpoint.
    """
    nome = fonte["nome"]
    log.info(f"=== {nome} ===")

    try:
        if skip_download:
            log.info(f"{nome}: --skip-download ativo, baixando bronze existente do Azure...")
            nomes_bronze = listar_blobs(f"bronze/{nome}")
            if not nomes_bronze:
                log.error(f"{nome}: nenhum blob encontrado em bronze/{nome}/ — não há o que reprocessar")
                return False
            tabulares = []
            for nome_interno in nomes_bronze:
                dados = baixar_bytes_do_blob(nome_interno, subpasta=f"bronze/{nome}")
                if dados is not None:
                    tabulares.append((nome_interno, dados))
            bronze_ok = True  # nada novo pra confirmar — já estava lá
        else:
            if skip_upload:
                conteudo, nome_arquivo = _baixar_para_memoria(fonte["url"])
                tabulares = _extrair_tabulares(conteudo, nome_arquivo)
                bronze_ok = False  # skip_upload -> bronze nunca é "confirmado"
            else:
                tabulares = processar_fonte_bronze(fonte)
                bronze_ok = True
    except Exception as e:
        log.error(f"{nome}: falha na etapa bronze — {type(e).__name__}: {e}")
        traceback.print_exc()
        return False

    try:
        df_silver = processar_fonte_silver(fonte, tabulares)
    except Exception as e:
        log.error(f"{nome}: falha na etapa silver — {type(e).__name__}: {e}")
        traceback.print_exc()
        return False

    silver_ok = True
    if not skip_upload:
        silver_ok = upload_dataframe_parquet(df_silver, f"{nome}.parquet", subpasta="silver")
    else:
        log.info(f"{nome}: --skip-upload ativo, silver não enviada")

    return bronze_ok and silver_ok and not skip_upload


# ------------------------------------------------------------------
# CLI / main
# ------------------------------------------------------------------
def parse_args():
    parser = argparse.ArgumentParser(description="Ingestão SEADE (investimentos) → bronze/silver no Azure")
    parser.add_argument("--fontes", nargs="+", default=None,
                         help="Nomes das fontes a processar (padrão: todas as cadastradas em FONTES)")
    parser.add_argument("--modo", choices=["append", "overwrite"], default="append",
                         help="append: pula fontes já concluídas no checkpoint | overwrite: reprocessa tudo")
    parser.add_argument("--skip-download", action="store_true",
                         help="Não baixa da fonte original — reprocessa a silver a partir do bronze já no Azure")
    parser.add_argument("--skip-upload", action="store_true",
                         help="Não sobe bronze/silver para o Azure. O checkpoint não avança — use só para testes.")
    return parser.parse_args()


def main():
    args = parse_args()

    if args.modo == "overwrite" and CHECKPOINT_PATH.exists():
        CHECKPOINT_PATH.unlink()

    checkpoint = carregar_checkpoint()
    log.info(f"Checkpoint: {len(checkpoint):,} fonte(s) já concluída(s) em execuções anteriores")
    if args.skip_upload:
        log.info("--skip-upload ativo: nada será enviado ao Azure, o checkpoint não avança nesta execução.")

    fontes_alvo = FONTES
    if args.fontes:
        nomes_validos = {f["nome"] for f in FONTES}
        desconhecidas = set(args.fontes) - nomes_validos
        if desconhecidas:
            log.warning(f"Fonte(s) desconhecida(s), ignoradas: {sorted(desconhecidas)}")
        fontes_alvo = [f for f in FONTES if f["nome"] in args.fontes]

    pendentes = [f for f in fontes_alvo if f["nome"] not in checkpoint]
    puladas = len(fontes_alvo) - len(pendentes)
    log.info(f"{len(pendentes)} fonte(s) a processar" + (f" ({puladas} já concluída(s) via checkpoint)" if puladas else ""))

    falhas = []
    for fonte in pendentes:
        sucesso = processar_fonte(fonte, skip_download=args.skip_download, skip_upload=args.skip_upload)
        if sucesso:
            checkpoint.add(fonte["nome"])
            salvar_checkpoint(checkpoint)
        else:
            falhas.append(fonte["nome"])

    if falhas:
        log.warning(f"Fonte(s) com falha ({len(falhas)}), serão retentadas no próximo --modo append: {falhas}")
    log.info(f"Concluído: {len(checkpoint)}/{len(FONTES)} fonte(s) no checkpoint")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log.warning("Interrompido manualmente (Ctrl+C). O checkpoint local só marca uma fonte como "
                    "concluída DEPOIS que bronze e silver são confirmados no Azure — o que não deu "
                    "tempo de subir será refeito automaticamente na próxima execução em --modo append.")
    except Exception:
        log.error("O script encerrou por causa de um erro não previsto:")
        traceback.print_exc()
        raise