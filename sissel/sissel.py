"""
sissel.py

Download e tratamento das bases de Alvarás do SISSEL (SMUL, Prefeitura de
SP). Adaptado de notebook Databricks/Spark (Unity Catalog Volumes/
Workspace) para Python puro, no mesmo padrão arquitetural do
pncp.py/tce.py/seade.py.

Descoberta de URLs: SISSEL_URLS_ANUAIS é gerado automaticamente por
gerar_sissel_urls(), combinando URLs fixas hardcoded (2000-2023, não
mudam) com URLs recentes (2024+) extraídas via scraping da página do
portal SMUL. Isso ACONTECE de verdade fazer scraping (parsing de HTML,
diferente do download em si dos arquivos XLS/XLSX, que é download direto)
— por isso é resiliente por padrão: se o portal SMUL estiver fora do ar ou
mudar de layout, o script não trava, só segue com as URLs fixas e avisa.

Arquitetura:
  - bronze: um arquivo XLS/XLSX por ano, baixado do SISSEL e enviado CRU
    para o Azure (sem nenhuma transformação).
      {AZURE_STORAGE_CONTAINER}/{AZURE_BLOB_PREFIX}/bronze/{ano}.xls(x)
  - silver: TODOS os anos disponíveis (baixados nesta execução ou já
    existentes no Azure) são lidos, concatenados, passam pela mesma
    unificação de colunas do notebook original (schema evolution entre
    anos antigos/novos), classificação de uso, e deduplicação — sempre
    reprocessada por completo a cada execução, sem checkpoint próprio.
      {AZURE_STORAGE_CONTAINER}/{AZURE_BLOB_PREFIX}/silver/sissel_alvaras.parquet

REGRA ESPECIAL herdada do notebook original: o ano corrente é SEMPRE
rebaixado, mesmo se já estiver no checkpoint — o arquivo do SISSEL acumula
meses ao longo do ano, então o arquivo de janeiro já baixado fica
desatualizado em fevereiro. Isso é intencional, não um bug.

Sem cópia local persistente de dado nenhum — só o checkpoint
(data/checkpoint_sissel.json) marca quais anos já tiveram o bronze
confirmado no Azure.

Uso:
  python sissel.py                              # todos os anos cadastrados
  python sissel.py --anos 2023 2024 2025
  python sissel.py --modo overwrite              # ignora checkpoint, rebaixa tudo
  python sissel.py --skip-ingest                 # só regenera a silver a partir do bronze existente
  python sissel.py --skip-upload                 # teste pontual — checkpoint não avança

.env na raiz do projeto (um nível acima da pasta deste script), não versionar:
  AZURE_STORAGE_CONNECTION_STRING=...
  ou
  AZURE_STORAGE_ACCOUNT_NAME=...
  AZURE_STORAGE_ACCOUNT_KEY=...
  AZURE_STORAGE_CONTAINER=conteiner
  AZURE_BLOB_PREFIX=sissel
"""

import argparse
import io
import json
import logging
import os
import re
import time
import traceback
from datetime import date, datetime
from pathlib import Path
from urllib.parse import quote

import numpy as np
import pandas as pd
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv

try:
    from azure.storage.blob import BlobServiceClient
except ImportError:
    BlobServiceClient = None

# ------------------------------------------------------------------
# Config
# ------------------------------------------------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))  # .../sissel/
ENV_PATH = os.path.normpath(os.path.join(BASE_DIR, "..", ".env"))
load_dotenv(ENV_PATH, override=True)

AZURE_CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
AZURE_ACCOUNT_NAME = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
AZURE_ACCOUNT_KEY = os.getenv("AZURE_STORAGE_ACCOUNT_KEY")
AZURE_CONTAINER = os.getenv("AZURE_STORAGE_CONTAINER", "conteiner")
AZURE_BLOB_PREFIX = os.getenv("AZURE_BLOB_PREFIX_sissel", "sissel").strip("/")

DATA_DIR = Path(os.path.normpath(os.path.join(BASE_DIR, "..", "data")))
CHECKPOINT_PATH = Path(os.getenv("CHECKPOINT_PATH", str(DATA_DIR / "checkpoint_sissel.json")))

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("sissel")
for _azure_logger in ("azure", "azure.core.pipeline.policies.http_logging_policy"):
    logging.getLogger(_azure_logger).setLevel(logging.WARNING)

log.info(f".env carregado de: {ENV_PATH} (existe: {os.path.exists(ENV_PATH)})")
log.info(f"Container: {AZURE_CONTAINER} | prefixo: {AZURE_BLOB_PREFIX}")

# ── Geração do dicionário {ano: url} ──────────────────────────────────────
# URLs fixas para anos anteriores a 2024 — não mudam
SISSEL_URLS_FIXAS = {
    "2023": "https://www.prefeitura.sp.gov.br/cidade/secretarias/upload/licenciamento/Ano_2023_SISSEL.xls",
    "2022": "https://www.prefeitura.sp.gov.br/cidade/secretarias/upload/licenciamento/ANUAL - 2022.xlsx",
    "2021": "https://www.prefeitura.sp.gov.br/cidade/secretarias/upload/licenciamentos/anual_2021_ate_dezembro.xls",
    "2020": "https://www.prefeitura.sp.gov.br/cidade/secretarias/upload/licenciamentos/Anual2020_ate_dezembro.xls",
    "2019": "https://www.prefeitura.sp.gov.br/cidade/secretarias/upload/licenciamentos/Anual2019.xls",
    "2018": "https://www.prefeitura.sp.gov.br/cidade/secretarias/upload/licenciamentos/Anual_2018.xls",
    "2017": "https://www.prefeitura.sp.gov.br/cidade/secretarias/upload/licenciamentos/Anual_2017_dezembro.xls",
    "2016": "https://www.prefeitura.sp.gov.br/cidade/secretarias/upload/anual_2016.xls",
    "2015": "https://www.prefeitura.sp.gov.br/cidade/secretarias/upload/licenciamento/2015 - anual.xls",
    "2014": "https://www.prefeitura.sp.gov.br/cidade/secretarias/upload/licenciamento/2014 - anual.xls",
    "2013": "https://www.prefeitura.sp.gov.br/cidade/secretarias/upload/licenciamento/2013 - anual.xls",
    "2012": "https://www.prefeitura.sp.gov.br/cidade/secretarias/upload/licenciamentos/anual_2012.xls",
    "2011": "https://www.prefeitura.sp.gov.br/cidade/secretarias/upload/licenciamentos/Anual2011.xls",
    "2010": "https://www.prefeitura.sp.gov.br/cidade/secretarias/upload/licenciamentos/anual2010.xls",
    "2009": "https://www.prefeitura.sp.gov.br/cidade/secretarias/upload/licenciamentos/anual2009.xls",
    "2008": "https://www.prefeitura.sp.gov.br/cidade/secretarias/upload/licenciamentos/anual2008.xls",
    "2007": "https://www.prefeitura.sp.gov.br/cidade/secretarias/upload/licenciamentos/anual2007.xls",
    "2006": "https://www.prefeitura.sp.gov.br/cidade/secretarias/upload/licenciamentos/anual2006.xls",
    "2005": "https://www.prefeitura.sp.gov.br/cidade/secretarias/upload/licenciamentos/anual2005.xls",
    "2004": "https://www.prefeitura.sp.gov.br/cidade/secretarias/upload/licenciamentos/anual2004.xls",
    "2003": "https://www.prefeitura.sp.gov.br/cidade/secretarias/upload/licenciamentos/Anual_2003.xls",
    "2002": "https://www.prefeitura.sp.gov.br/cidade/secretarias/upload/licenciamentos/anual_2002.xls",
    "2001": "https://www.prefeitura.sp.gov.br/cidade/secretarias/upload/licenciamentos/anual_2001.xls",
    "2000": "https://www.prefeitura.sp.gov.br/cidade/secretarias/upload/licenciamentos/anual_2000.xls",
}

SISSEL_PAGINA = "https://prefeitura.sp.gov.br/licenciamento/w/servicos/3334"


def _scrape_urls_recentes() -> dict:
    """
    Extrai do portal os links anuais acumulados de 2024 em diante (formato
    novo: sissel_ano_YYYY[_MM]). Para cada ano, prioriza o arquivo com o mês
    mais recente (ex: sissel_ano_2026_03 > sissel_ano_2026_02).

    Isso É scraping de verdade (parsing de HTML, não uma API) — por isso é
    resiliente por padrão: se o portal estiver fora do ar ou mudar de
    layout, não derruba o script inteiro, só retorna vazio e segue com as
    URLs fixas (2000-2023).
    """
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
        "Referer": SISSEL_PAGINA,
    })

    resp = None
    for tentativa in range(1, 4):
        try:
            resp = session.get(SISSEL_PAGINA, timeout=30)
            resp.raise_for_status()
            break
        except Exception as e:
            if tentativa == 3:
                log.warning(f"Não consegui acessar o portal SMUL após 3 tentativas "
                            f"({type(e).__name__}: {e}) — seguindo só com as URLs fixas (2000-2023)")
                return {}
            espera = 10 * tentativa
            log.warning(f"Falha ao acessar o portal SMUL — aguardando {espera}s (tentativa {tentativa}/3)")
            time.sleep(espera)

    try:
        soup = BeautifulSoup(resp.text, "html.parser")
    except Exception as e:
        log.warning(f"Falha ao interpretar a página do portal SMUL: {type(e).__name__}: {e} — "
                    f"seguindo só com URLs fixas (2000-2023)")
        return {}

    candidatos = {}  # ano → {mes → href}
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "/documents/d/licenciamento/sissel" not in href:
            continue
        if href.startswith("/"):
            href = "https://prefeitura.sp.gov.br" + href

        match = re.search(r"sissel_ano_(\d{4})(?:_(\d{2}))?", href)
        if not match:
            continue

        ano = match.group(1)
        mes = int(match.group(2)) if match.group(2) else 0
        candidatos.setdefault(ano, {})[mes] = href

    urls_recentes = {}
    for ano, meses in candidatos.items():
        mes_mais_recente = max(meses.keys())
        urls_recentes[ano] = meses[mes_mais_recente]
        log.info(f"  {ano}: mês {mes_mais_recente:02d} → {urls_recentes[ano]}")

    if not candidatos:
        log.warning("Nenhum link 'sissel_ano_*' encontrado na página do portal — layout pode ter mudado, "
                     "ou não há anos recentes publicados ainda. Seguindo só com URLs fixas.")

    return urls_recentes


def gerar_sissel_urls() -> dict:
    """
    Monta o dicionário completo combinando:
      - URLs fixas (2000-2023): não mudam, hardcoded
      - URLs recentes (2024+): extraídas dinamicamente da página do portal
    O arquivo mais recente do ano corrente é sempre atualizado (mês mais
    recente disponível no portal).
    """
    log.info("Buscando URLs recentes no portal SMUL...")
    urls_recentes = _scrape_urls_recentes()
    urls_completo = {**SISSEL_URLS_FIXAS, **urls_recentes}
    log.info(f"Total de anos mapeados: {len(urls_completo)} — {sorted(urls_completo.keys())}")
    return urls_completo


# SISSEL_URLS_ANUAIS começa vazio e só é preenchido dentro de main(), na
# primeira vez que for necessário — evita fazer uma requisição de rede
# desnecessária ao portal SMUL toda vez que o script é importado ou chamado
# só com --help, e facilita testes (dá pra popular manualmente antes de
# chamar main()).
SISSEL_URLS_ANUAIS = {}

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
    "Referer": "https://prefeitura.sp.gov.br/licenciamento/w/servicos/3334",
})

PALAVRAS_HEADER = {"mes", "mês", "alvara", "alvará", "unidade", "bairro", "descricao", "descrição"}


# ------------------------------------------------------------------
# Checkpoint local (só a bronze tem — silver é sempre full-reprocess)
# ------------------------------------------------------------------
def carregar_checkpoint() -> set:
    if not CHECKPOINT_PATH.exists():
        return set()
    try:
        with open(CHECKPOINT_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        return set(data.get("anos_concluidos", []))
    except Exception as e:
        log.warning(f"Checkpoint '{CHECKPOINT_PATH}' ilegível ({type(e).__name__}) — iniciando do zero")
        return set()


def salvar_checkpoint(concluidos: set):
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    tmp_path = CHECKPOINT_PATH.with_suffix(".tmp")
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(
            {"anos_concluidos": sorted(concluidos),
             "atualizado_em": datetime.now().isoformat(timespec="seconds")},
            f, indent=2, ensure_ascii=False,
        )
    tmp_path.replace(CHECKPOINT_PATH)


# ------------------------------------------------------------------
# Azure Blob Storage — helpers em memória, mesmo padrão dos outros pipelines
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
            log.info(f"Upload OK (bronze) → '{blob_name}' ({len(dados) / 1024:.1f} KB)")
            return True
        except Exception as e:
            if tentativa == 3:
                log.error(f"Falha no upload de '{blob_name}' após 3 tentativas: {type(e).__name__}: {e}")
                return False
            log.warning(f"Upload de '{blob_name}' falhou (tentativa {tentativa}/3) — aguardando 15s... ({e})")
            time.sleep(15)
    return False


def baixar_bytes_do_blob(nome_arquivo: str, subpasta: str):
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


def upload_dataframe_parquet(df: pd.DataFrame, nome_arquivo: str, subpasta: str = "") -> bool:
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
# Download bronze (equivalente a baixar_e_salvar do notebook original)
# ------------------------------------------------------------------
def _url_extensao(url: str) -> str:
    return ".xlsx" if url.endswith(("-xlsx", ".xlsx")) else ".xls"


def _baixar_ano(ano: str, url: str) -> bytes:
    """Baixa o arquivo do ano. Tenta 3x em caso de falha de conexão."""
    url_encoded = quote(url, safe=":/?=&#%")
    for tentativa in range(1, 4):
        try:
            resp = SESSION.get(url_encoded, timeout=120)
            resp.raise_for_status()
            return resp.content
        except Exception as e:
            if tentativa == 3:
                raise
            espera = 10 * tentativa
            log.warning(f"{ano}: {type(e).__name__} — aguardando {espera}s (tentativa {tentativa}/3)")
            time.sleep(espera)


def ingest_sissel_bronze(anos: list, modo: str, skip_upload: bool = False) -> dict:
    """
    Baixa/atualiza o bronze de cada ano pedido. Retorna {ano: bytes} com o
    conteúdo de TODOS os anos (recém-baixados nesta execução, ou
    reaproveitados do Azure para os que já estavam no checkpoint).

    Regra especial: o ano corrente é sempre rebaixado, mesmo que já esteja
    no checkpoint (o arquivo do SISSEL acumula meses ao longo do ano).

    GARANTIA: um ano só entra no checkpoint depois que o upload do bronze é
    confirmado no Azure — nunca antes.
    """
    if modo == "overwrite" and CHECKPOINT_PATH.exists():
        CHECKPOINT_PATH.unlink()

    checkpoint = carregar_checkpoint()
    ano_corrente = str(date.today().year)
    log.info(f"Checkpoint: {len(checkpoint):,} ano(s) já concluído(s) em execuções anteriores")
    if skip_upload:
        log.info("--skip-upload ativo: nada será enviado ao Azure, o checkpoint não avança nesta execução.")

    bronze_bytes = {}

    for ano in sorted(anos):
        url = SISSEL_URLS_ANUAIS.get(ano)
        if url is None:
            log.warning(f"{ano}: sem URL cadastrada em SISSEL_URLS_ANUAIS — pulando")
            continue

        ja_concluido = modo != "overwrite" and ano in checkpoint
        e_ano_corrente = ano == ano_corrente

        if ja_concluido and not e_ano_corrente:
            log.info(f"{ano}: já concluído (checkpoint) — reaproveitando bronze existente no Azure")
            dados = baixar_bytes_do_blob(f"{ano}{_url_extensao(url)}", subpasta="bronze")
            if dados is not None:
                bronze_bytes[ano] = dados
                continue
            log.warning(f"{ano}: checkpoint dizia que existia, mas não achei no Azure — baixando de novo")

        if e_ano_corrente and ja_concluido:
            log.info(f"{ano}: é o ano corrente — rebaixando mesmo já estando no checkpoint "
                     f"(arquivo acumula mês a mês)")

        try:
            conteudo = _baixar_ano(ano, url)
        except Exception as e:
            log.error(f"{ano}: download falhou — {type(e).__name__}: {e}")
            continue

        log.info(f"{ano}: {len(conteudo) / 1024:.1f} KB baixados")
        nome_arquivo = f"{ano}{_url_extensao(url)}"

        if skip_upload:
            bronze_bytes[ano] = conteudo
            continue

        sucesso = upload_bytes_para_blob(conteudo, nome_arquivo, subpasta="bronze")
        if sucesso:
            bronze_bytes[ano] = conteudo
            checkpoint.add(ano)
            salvar_checkpoint(checkpoint)
        else:
            log.warning(f"{ano}: upload do bronze falhou — não entra no checkpoint, será refeito")

    return bronze_bytes


def _coletar_bronze_do_azure(anos: list) -> dict:
    """Usado com --skip-ingest: baixa o bronze de cada ano direto do Azure,
    sem tocar a fonte SISSEL."""
    bronze_bytes = {}
    for ano in sorted(anos):
        url = SISSEL_URLS_ANUAIS.get(ano)
        nome = f"{ano}{_url_extensao(url)}" if url else f"{ano}.xlsx"
        dados = baixar_bytes_do_blob(nome, subpasta="bronze")
        if dados is not None:
            bronze_bytes[ano] = dados
        else:
            log.warning(f"{ano}: bronze não encontrado no Azure ('{nome}')")
    return bronze_bytes


# ------------------------------------------------------------------
# Leitura/normalização por ano (equivalente a ler_xls do notebook original)
# ------------------------------------------------------------------
def _normalizar_colunas(df: pd.DataFrame) -> pd.DataFrame:
    """Normaliza nomes de coluna preservando acentos (o passo de unificar
    variantes acentuadas/sem acento acontece depois, na silver, via
    coalesce). Só limpa espaço/símbolo/maiúscula — mesma regra do original."""
    df.columns = [str(c) for c in df.columns]
    cols = pd.Index(df.columns)
    cols = cols.str.strip().str.lower()
    cols = cols.str.replace(r"[\s/\n\(\)²%\.]+", "_", regex=True)
    cols = cols.str.replace(r"_+", "_", regex=True)
    cols = cols.str.strip("_")
    df.columns = cols
    return df


def ler_xls(conteudo: bytes, ano: str, nome_arquivo: str) -> pd.DataFrame:
    ext = ".xlsx" if nome_arquivo.endswith(".xlsx") else ".xls"
    engine = "openpyxl" if ext == ".xlsx" else "xlrd"

    xls = pd.ExcelFile(io.BytesIO(conteudo), engine=engine)
    abas = xls.sheet_names

    # Escolhe a aba com mais linhas — independente do nome
    aba_escolhida = abas[0]
    max_linhas = 0
    for aba in abas:
        try:
            df_teste = pd.read_excel(io.BytesIO(conteudo), engine=engine, dtype=str,
                                      header=None, sheet_name=aba)
            if len(df_teste) > max_linhas:
                max_linhas = len(df_teste)
                aba_escolhida = aba
        except Exception:
            continue

    log.info(f"{ano}: abas {abas} → escolhida '{aba_escolhida}' ({max_linhas} linhas)")

    df_raw = pd.read_excel(io.BytesIO(conteudo), engine=engine, dtype=str,
                            header=None, sheet_name=aba_escolhida)

    # Detecta a linha do header real (algumas planilhas têm linhas de título antes)
    header_row = 0
    for i, row in df_raw.iterrows():
        valores = {str(v).strip().lower() for v in row.values if pd.notna(v)}
        if len(PALAVRAS_HEADER & valores) >= 2:
            header_row = i
            break

    df = pd.read_excel(io.BytesIO(conteudo), engine=engine, dtype=str,
                        header=header_row, sheet_name=aba_escolhida)

    df = df.dropna(how="all")
    df = df.loc[:, ~df.columns.astype(str).str.startswith("Unnamed")]
    df.columns = [str(c) for c in df.columns]
    df = _normalizar_colunas(df)
    df["ano_referencia"] = ano
    df["dt_ingestao"] = str(date.today())

    return df


def montar_raw(bronze_bytes: dict) -> pd.DataFrame:
    """Lê e concatena todos os anos disponíveis em bronze_bytes."""
    frames = []
    erros = []

    for ano in sorted(bronze_bytes.keys()):
        conteudo = bronze_bytes[ano]
        url = SISSEL_URLS_ANUAIS.get(ano, "")
        nome_arquivo = f"{ano}{_url_extensao(url)}" if url else f"{ano}.xlsx"
        try:
            df = ler_xls(conteudo, ano, nome_arquivo)
            frames.append(df)
            log.info(f"{ano}: {len(df):,} linhas x {len(df.columns)} colunas")
        except Exception as e:
            erros.append(ano)
            log.error(f"{ano}: falha ao ler — {type(e).__name__}: {e}")
            traceback.print_exc()

    if not frames:
        raise ValueError("Nenhum arquivo bronze pôde ser lido — silver não gerada")

    df_total = pd.concat(frames, ignore_index=True)
    log.info(f"Total raw: {len(df_total):,} registros, {len(erros)} ano(s) com erro de leitura")
    if erros:
        log.warning(f"Anos com erro na leitura: {erros}")
    return df_total


# ------------------------------------------------------------------
# Silver — unificação de schema entre anos (equivalente a
# transform_silver_sissel do notebook original, em pandas)
# ------------------------------------------------------------------
def _coalesce(df: pd.DataFrame, *nomes) -> pd.Series:
    """Equivalente ao F.coalesce do Spark: primeiro valor não-nulo entre as
    colunas citadas, na ordem dada. Colunas ausentes (schema evolution
    entre anos) contam como totalmente nulas, sem dar erro."""
    resultado = pd.Series(pd.NA, index=df.index, dtype="object")
    for nome in nomes:
        if nome in df.columns:
            resultado = resultado.where(resultado.notna(), df[nome])
    return resultado


def _concat_ws(sep: str, *series_list) -> pd.Series:
    """Equivalente ao F.concat_ws do Spark: junta os valores não-nulos de
    cada linha com o separador, ignorando nulos (não deixa '// ' sobrando)."""
    combinado = pd.concat(series_list, axis=1)

    def _unir(row):
        partes = [str(v) for v in row if pd.notna(v) and str(v).strip() != ""]
        return sep.join(partes) if partes else None

    return combinado.apply(_unir, axis=1)


def _parse_data_multi_formato(serie: pd.Series) -> pd.Series:
    """Tenta dd/MM/yyyy primeiro, depois dd/MM/yy nos que sobraram —
    equivalente ao coalesce(try_to_date(...), try_to_date(...)) do Spark."""
    d1 = pd.to_datetime(serie, format="%d/%m/%Y", errors="coerce")
    faltando = d1.isna() & serie.notna()
    if faltando.any():
        d2 = pd.to_datetime(serie[faltando], format="%d/%m/%y", errors="coerce")
        d1.loc[faltando] = d2
    return d1


def _serie_ou_vazia(df: pd.DataFrame, nome: str) -> pd.Series:
    return df[nome] if nome in df.columns else pd.Series(pd.NA, index=df.index)


def transformar_silver_sissel(df: pd.DataFrame) -> pd.DataFrame:
    silver = pd.DataFrame(index=df.index)

    silver["ano_referencia"] = _serie_ou_vazia(df, "ano_referencia")
    silver["dt_ingestao"] = _serie_ou_vazia(df, "dt_ingestao")

    silver["mes"] = _coalesce(df, "mês", "meses")
    silver["unidade"] = _serie_ou_vazia(df, "unidade")
    silver["administracao_regional"] = _coalesce(
        df, "administração_regional", "administracao_regional", "subprefeitura", "prefeitura_regional"
    )
    silver["alvara"] = _coalesce(df, "alvará", "alvara")
    silver["processo"] = _serie_ou_vazia(df, "processo")
    silver["descricao"] = _coalesce(df, "descrição", "descricao")
    silver["tipo_da_construcao"] = _serie_ou_vazia(df, "tipo_da_construcao")
    silver["sql_incra"] = _serie_ou_vazia(df, "sql_incra")
    silver["categoria_de_uso"] = _serie_ou_vazia(df, "categoria_de_uso")
    silver["zona_de_uso_atual"] = _coalesce(df, "zona_de_uso_atual", "zona_de_uso")
    silver["zona_de_uso_anterior"] = _serie_ou_vazia(df, "zona_de_uso_anterior")
    silver["bairro"] = _serie_ou_vazia(df, "bairro")
    silver["endereco"] = _coalesce(df, "endereço", "endereco")

    area_construcao_str = _coalesce(df, "área_da_construção_m", "area_da_construcao")
    area_terreno_str = _coalesce(df, "área_do_terreno_m", "area_do_terreno")
    # remove separador de milhar "." antes de trocar decimal ","->"."
    silver["area_construcao_m2"] = pd.to_numeric(
        area_construcao_str.astype(str).str.replace(".", "", regex=False).str.replace(",", ".", regex=False),
        errors="coerce",
    )
    silver["area_terreno_m2"] = pd.to_numeric(
        area_terreno_str.astype(str).str.replace(".", "", regex=False).str.replace(",", ".", regex=False),
        errors="coerce",
    )

    silver["proprietario"] = _coalesce(df, "proprietário", "proprietario")

    silver["dt_aprovacao"] = _parse_data_multi_formato(_coalesce(df, "aprovação", "aprovacao"))
    silver["dt_autuacao"] = _parse_data_multi_formato(_coalesce(df, "data_autuação", "dtautuacao"))

    silver["dirigente_tecnico"] = _coalesce(df, "dirigente_técnico", "firma_dirigente_tecnico")
    silver["responsavel_empresa"] = _coalesce(df, "responsável_pela_empresa", "responsavel_da_firma")
    silver["autor_projeto"] = _coalesce(df, "autor_do_projeto", "autor_projeto")
    silver["responsavel_tecnico"] = _serie_ou_vazia(df, "responsavel_tecnico")

    blocos_concat = _concat_ws(
        " / ",
        _serie_ou_vazia(df, "numero_de_blocos"),
        _serie_ou_vazia(df, "numero_de_pavimentos"),
        _serie_ou_vazia(df, "numero_de_unidades"),
    )
    blocos_direto = _serie_ou_vazia(df, "b_locos_p_avimentos_u_nidades")
    silver["blocos_pavimentos_unidades"] = blocos_direto.where(blocos_direto.notna(), blocos_concat)

    # ── Classificação de uso ──────────────────────────────────────────────
    categoria = silver["categoria_de_uso"].astype(str)
    condicoes = [
        categoria.str.contains(r"R[1-4]|resid|HIS|HMP", case=False, regex=True, na=False),
        categoria.str.contains(r"C[1-4]|comerc", case=False, regex=True, na=False),
        categoria.str.contains(r"I[1-4]|indust", case=False, regex=True, na=False),
        categoria.str.contains(r"E[1-3]|equip", case=False, regex=True, na=False),
    ]
    escolhas = ["residencial", "comercial", "industrial", "equipamento"]
    silver["uso_classificado"] = np.select(condicoes, escolhas, default="outros")

    silver = silver.drop_duplicates(subset=["alvara", "processo", "ano_referencia"])

    return silver


def ingest_silver(bronze_bytes: dict, skip_upload: bool = False) -> pd.DataFrame:
    df_raw = montar_raw(bronze_bytes)
    silver = transformar_silver_sissel(df_raw)
    log.info(f"{len(silver):,} registros silver gerados em memória")

    if not skip_upload:
        upload_dataframe_parquet(silver, "sissel_alvaras.parquet", subpasta="silver")
    else:
        log.info("--skip-upload ativo, silver não enviada")

    return silver


# ------------------------------------------------------------------
# CLI / main
# ------------------------------------------------------------------
def parse_args():
    parser = argparse.ArgumentParser(description="Ingestão SISSEL (Alvarás SMUL) → bronze/silver no Azure")
    parser.add_argument("--anos", nargs="+", default=None,
                         help="Anos a processar, ex: --anos 2023 2024 2025 (padrão: todos em SISSEL_URLS_ANUAIS)")
    parser.add_argument("--modo", choices=["append", "overwrite"], default="append",
                         help="append: pula anos já concluídos no checkpoint (exceto o ano corrente) | "
                              "overwrite: reprocessa tudo do zero")
    parser.add_argument("--skip-ingest", action="store_true",
                         help="Não baixa da fonte SISSEL — baixa o bronze existente no Azure e só regenera a silver")
    parser.add_argument("--skip-upload", action="store_true",
                         help="Não sobe bronze/silver para o Azure. O checkpoint não avança — use só para testes.")
    return parser.parse_args()


def main():
    global SISSEL_URLS_ANUAIS
    args = parse_args()

    if not SISSEL_URLS_ANUAIS:
        SISSEL_URLS_ANUAIS = gerar_sissel_urls()

    anos = args.anos or sorted(SISSEL_URLS_ANUAIS.keys())

    if not anos:
        log.error("Nenhum ano disponível para processar — SISSEL_URLS_ANUAIS ficou vazio "
                   "(portal fora do ar e nenhuma URL fixa? confira a conexão) ou use --anos")
        return

    log.info(f"Anos: {anos}")
    log.info(f"Modo: {args.modo}")
    log.info(f"Upload Azure: {'desabilitado (--skip-upload)' if args.skip_upload else ('configurado' if _azure_configurado() else 'sem credenciais no .env')}")

    if args.skip_ingest:
        log.info("=== Bronze SISSEL (via --skip-ingest, direto do Azure) ===")
        bronze_bytes = _coletar_bronze_do_azure(anos)
    else:
        log.info("=== Bronze SISSEL ===")
        bronze_bytes = ingest_sissel_bronze(anos, args.modo, skip_upload=args.skip_upload)

    if not bronze_bytes:
        log.error("Nenhum bronze disponível — nada a processar")
        return

    log.info("=== Silver SISSEL ===")
    silver = ingest_silver(bronze_bytes, skip_upload=args.skip_upload)

    resumo = (
        silver.groupby(["uso_classificado", "ano_referencia"])
              .agg(qtd=("alvara", "count"),
                   area_total_km2=("area_construcao_m2", lambda s: round(s.sum() / 1e6, 2)))
              .reset_index()
              .sort_values(["ano_referencia", "qtd"], ascending=[True, False])
    )
    log.info("Resumo por uso/ano:\n" + resumo.to_string(index=False))


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log.warning("Interrompido manualmente (Ctrl+C). O checkpoint local só marca um ano como "
                    "concluído DEPOIS que o upload do bronze é confirmado no Azure — o que não deu "
                    "tempo de subir será refeito automaticamente na próxima execução em --modo append.")
    except Exception:
        log.error("O script encerrou por causa de um erro não previsto:")
        traceback.print_exc()
        raise