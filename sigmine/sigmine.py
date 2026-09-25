"""
sigmine.py

Download e tratamento dos processos minerários de agregados (SP) do SIGMINE/
ANM (Agência Nacional de Mineração). Adaptado de notebook Databricks/Spark
(Unity Catalog) para Python puro, no mesmo padrão arquitetural do
pncp.py/tce.py/seade.py/sissel.py.

Fonte: API REST pública da ANM (ArcGIS FeatureServer, resposta em JSON) —
é uma API de verdade, com paginação por offset, não scraping.

Particularidade deste pipeline: são DUAS buscas paginadas na mesma fonte
("atributos" — dados do processo — e "geometria" — polígono da área, do
qual calculamos o centroide), que só se juntam na etapa silver.

Arquitetura:
  - bronze: duas seções independentes, cada uma como um único arquivo JSONL
    no Azure, mesclado em memória a cada execução (igual ao bronze do
    pncp.py/tce.py).
      {AZURE_STORAGE_CONTAINER}/{AZURE_BLOB_PREFIX}/bronze/atributos.jsonl
      {AZURE_STORAGE_CONTAINER}/{AZURE_BLOB_PREFIX}/bronze/geometrias.jsonl
  - silver: junta as duas seções (merge por "processo"), renomeia/tipa
    colunas, classifica fase_grupo e tipo_agregado, deduplica — sempre
    reprocessada por completo, sem checkpoint próprio.
      {AZURE_STORAGE_CONTAINER}/{AZURE_BLOB_PREFIX}/silver/sigmine_processos_sp.parquet

IMPORTANTE — semântica do checkpoint aqui é diferente dos outros pipelines:
este dado NÃO é particionado por data/ano/fonte — é uma consulta única que
traz o estado atual completo dos processos minerários. Por isso:
  - --modo overwrite (padrão, igual ao dropdown do notebook original):
    ignora qualquer progresso anterior e busca tudo de novo, do offset 0.
  - --modo append: NÃO significa "acumular dado novo ao longo do tempo"
    (não existe "novo período" aqui) — significa "retomar uma busca que
    foi interrompida no meio", continuando do último offset confirmado.
    Se não houver progresso incompleto de uma execução anterior, se
    comporta como overwrite (começa do zero) — não faz sentido "pular"
    uma atualização já concluída, já que os dados da ANM mudam com o tempo
    e cada execução deveria buscar o estado mais atual.

GARANTIA (igual aos outros pipelines): um lote de páginas só avança o
checkpoint depois que o upload correspondente é confirmado no Azure.

Uso:
  python sigmine.py                          # busca tudo, do zero (overwrite)
  python sigmine.py --modo append            # retoma uma execução anterior interrompida
  python sigmine.py --skip-ingest            # só regenera a silver a partir do bronze existente
  python sigmine.py --skip-upload            # teste pontual — checkpoint não avança
  python sigmine.py --lote-paginas 10        # sobe pro Azure a cada 10 páginas em vez de 5

.env na raiz do projeto (um nível acima da pasta deste script), não versionar:
  AZURE_STORAGE_CONNECTION_STRING=...
  ou
  AZURE_STORAGE_ACCOUNT_NAME=...
  AZURE_STORAGE_ACCOUNT_KEY=...
  AZURE_STORAGE_CONTAINER=conteiner
  AZURE_BLOB_PREFIX=sigmine
"""

import argparse
import io
import json
import logging
import os
import time
import traceback
from datetime import date, datetime
from pathlib import Path

import pandas as pd
import requests
from dotenv import load_dotenv

try:
    from azure.storage.blob import BlobServiceClient
except ImportError:
    BlobServiceClient = None

# ------------------------------------------------------------------
# Config
# ------------------------------------------------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))  # .../sigmine/
ENV_PATH = os.path.normpath(os.path.join(BASE_DIR, "..", ".env"))
load_dotenv(ENV_PATH, override=True)

AZURE_CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
AZURE_ACCOUNT_NAME = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
AZURE_ACCOUNT_KEY = os.getenv("AZURE_STORAGE_ACCOUNT_KEY")
AZURE_CONTAINER = os.getenv("AZURE_STORAGE_CONTAINER", "conteiner")
AZURE_BLOB_PREFIX = os.getenv("AZURE_BLOB_PREFIX_sigmine", "sigmine").strip("/")

DATA_DIR = Path(os.path.normpath(os.path.join(BASE_DIR, "..", "data")))
CHECKPOINT_PATH = Path(os.getenv("CHECKPOINT_PATH", str(DATA_DIR / "checkpoint_sigmine.json")))

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("sigmine")
for _azure_logger in ("azure", "azure.core.pipeline.policies.http_logging_policy"):
    logging.getLogger(_azure_logger).setLevel(logging.WARNING)

log.info(f".env carregado de: {ENV_PATH} (existe: {os.path.exists(ENV_PATH)})")
log.info(f"Container: {AZURE_CONTAINER} | prefixo: {AZURE_BLOB_PREFIX}")

ANM_BASE = "https://geo.anm.gov.br/arcgis/rest/services/SIGMINE/dados_anm/FeatureServer/0/query"

SUBS_AGREGADOS = (
    "BASALTO", "GRANITO", "CALCÁRIO", "CALCÁRIO DOLOMÍTICO",
    "DOLOMITO", "QUARTZITO", "ARENITO", "MÁRMORE",
    "AREIA", "AREIA INDUSTRIAL", "AREIA QUARTZOSA",
    "CASCALHO", "SAIBRO",
)
_SUBS_FILTER = ", ".join(f"'{s}'" for s in SUBS_AGREGADOS)
WHERE = f"UF='SP' AND SUBS IN ({_SUBS_FILTER})"

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://geo.anm.gov.br/portal/apps/webappviewer/index.html",
})

_sessao_aquecida = False


def _aquecer_sessao():
    """Visita a home do portal antes da 1ª chamada à API — alguns WAFs só
    liberam requisições subsequentes depois de ver um cookie de sessão
    obtido numa visita normal ao site. Sem efeito nenhum se não for
    necessário; falha em silêncio (não é crítico — a chamada real à API
    vai reportar o erro de verdade se o bloqueio persistir)."""
    global _sessao_aquecida
    if _sessao_aquecida:
        return
    try:
        SESSION.get("https://geo.anm.gov.br/", timeout=15)
    except Exception as e:
        log.debug(f"Aquecimento de sessão falhou (não crítico): {type(e).__name__}: {e}")
    _sessao_aquecida = True

NOME_BRONZE_ATRIBUTOS = "atributos.jsonl"
NOME_BRONZE_GEOMETRIA = "geometrias.jsonl"
NOME_SILVER = "sigmine_processos_sp.parquet"

DELAY_ENTRE_PAGINAS = 0.3  # segundos, só por educação com a API


# ------------------------------------------------------------------
# Checkpoint local — aqui guarda, POR SEÇÃO, o offset já confirmado no
# Azure (não é um conjunto de chaves como nos outros pipelines, porque a
# paginação é sequencial sobre o mesmo conjunto de dados).
# ------------------------------------------------------------------
def carregar_checkpoint() -> dict:
    if not CHECKPOINT_PATH.exists():
        return {}
    try:
        with open(CHECKPOINT_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data.get("secoes", {})
    except Exception as e:
        log.warning(f"Checkpoint '{CHECKPOINT_PATH}' ilegível ({type(e).__name__}) — iniciando do zero")
        return {}


def salvar_checkpoint(secoes: dict):
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    tmp_path = CHECKPOINT_PATH.with_suffix(".tmp")
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(
            {"secoes": secoes, "atualizado_em": datetime.now().isoformat(timespec="seconds")},
            f, indent=2, ensure_ascii=False,
        )
    tmp_path.replace(CHECKPOINT_PATH)


# ------------------------------------------------------------------
# Azure Blob Storage — mesmo padrão dos outros pipelines
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


def baixar_jsonl_do_blob(nome_arquivo: str, subpasta: str) -> pd.DataFrame:
    client = get_blob_service_client()
    if client is None:
        return pd.DataFrame()
    blob_name = _blob_name(nome_arquivo, subpasta)
    try:
        container_client = client.get_container_client(AZURE_CONTAINER)
        blob_client = container_client.get_blob_client(blob_name)
        if not blob_client.exists():
            log.info(f"Nenhum bronze prévio em '{blob_name}' — começando do zero")
            return pd.DataFrame()
        conteudo = blob_client.download_blob().readall().decode("utf-8")
        registros = [json.loads(linha) for linha in conteudo.splitlines() if linha.strip()]
        df = pd.DataFrame(registros)
        log.info(f"Baixado '{blob_name}' da memória — {len(df):,} registro(s) prévio(s)")
        return df
    except Exception as e:
        log.error(f"Falha ao baixar '{blob_name}' do Azure: {type(e).__name__}: {e}")
        return pd.DataFrame()


def upload_dataframe_jsonl(df: pd.DataFrame, nome_arquivo: str, subpasta: str = "") -> bool:
    client = get_blob_service_client()
    if client is None:
        return False
    blob_name = _blob_name(nome_arquivo, subpasta)
    try:
        linhas = "\n".join(json.dumps(reg, ensure_ascii=False, default=str) for reg in df.to_dict("records"))
        dados = linhas.encode("utf-8")
        tamanho_mb = len(dados) / (1024 * 1024)
        container_client = _get_container_client(client)
        container_client.upload_blob(name=blob_name, data=dados, overwrite=True)
        log.info(f"Upload OK (bronze) → '{blob_name}' ({len(df):,} registros, {tamanho_mb:.2f} MB)")
        return True
    except Exception as e:
        log.error(f"Falha no upload de '{blob_name}': {type(e).__name__}: {e}")
        return False


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
        container_client.upload_blob(name=blob_name, data=buffer, overwrite=True)
        log.info(f"Upload OK (silver) → '{blob_name}' ({len(df):,} registros, {tamanho_mb:.2f} MB)")
        return True
    except Exception as e:
        log.error(f"Falha no upload de '{blob_name}': {type(e).__name__}: {e}")
        return False


# ------------------------------------------------------------------
# API SIGMINE/ANM (ArcGIS FeatureServer, JSON) — download por página
# ------------------------------------------------------------------
def _get_com_retry(params: dict, timeout: int = 60) -> dict:
    """GET na API com retry de conexão (3 tentativas, espera crescente).
    Erros HTTP 4xx (ex: 403 bloqueio de WAF) não são retentados — não
    adianta tentar de novo se o servidor está recusando ativamente, e isso
    evita queimar minutos em retry inútil."""
    for tentativa in range(1, 4):
        try:
            _aquecer_sessao()
            # POST em vez de GET: os mesmos parâmetros (incluindo o "where" com
            # sintaxe tipo SQL) vão no corpo da requisição, não na URL. Alguns
            # firewalls com assinatura de SQLi vasculham a query string da URL
            # de forma mais agressiva que o corpo de um POST — isso não é bypass
            # de segurança nenhum, só uma forma diferente (e igualmente válida
            # pela API do ArcGIS) de mandar o mesmo pedido.
            resp = SESSION.post(ANM_BASE, data=params, timeout=timeout)
            if 400 <= resp.status_code < 500:
                log.error(f"HTTP {resp.status_code} da API ANM — corpo da resposta: "
                          f"{resp.text[:300]!r}")
            resp.raise_for_status()
            return resp.json()
        except requests.exceptions.HTTPError as e:
            if 400 <= e.response.status_code < 500:
                raise  # não adianta retry — erro do cliente/bloqueio, não instabilidade
            if tentativa == 3:
                raise
            espera = 10 * tentativa
            log.warning(f"Chamada à API falhou ({type(e).__name__}) — aguardando {espera}s "
                        f"(tentativa {tentativa}/3)")
            time.sleep(espera)
        except Exception as e:
            if tentativa == 3:
                raise
            espera = 10 * tentativa
            log.warning(f"Chamada à API falhou ({type(e).__name__}) — aguardando {espera}s "
                        f"(tentativa {tentativa}/3)")
            time.sleep(espera)


def contar_total() -> int:
    data = _get_com_retry({"where": WHERE, "returnCountOnly": "true", "f": "json"}, timeout=30)
    return data.get("count", 0)


def fetch_atributos_pagina(offset: int, page_size: int) -> list:
    hoje = str(date.today())
    data = _get_com_retry({
        "where": WHERE, "outFields": "*", "f": "json",
        "resultOffset": offset, "resultRecordCount": page_size,
        "orderByFields": "FID",
    })
    registros = []
    for feature in data.get("features", []):
        attrs = dict(feature["attributes"])
        attrs["dt_ingestao"] = hoje
        registros.append(attrs)
    return registros


def _calcular_centroide(rings: list) -> tuple:
    """Centroide simples: média das coordenadas do primeiro anel do polígono."""
    if not rings or not rings[0]:
        return None, None
    coords = rings[0]
    lon = sum(c[0] for c in coords) / len(coords)
    lat = sum(c[1] for c in coords) / len(coords)
    return round(lat, 6), round(lon, 6)


def fetch_geometria_pagina(offset: int, page_size: int) -> list:
    hoje = str(date.today())
    data = _get_com_retry({
        "where": WHERE, "outFields": "PROCESSO", "f": "json",
        "returnGeometry": "true", "outSR": "4326",
        "resultOffset": offset, "resultRecordCount": page_size,
        "orderByFields": "FID",
    })
    registros = []
    for feature in data.get("features", []):
        processo = feature["attributes"]["PROCESSO"]
        geometry = feature.get("geometry", {})
        rings = geometry.get("rings", [])
        lat, lon = _calcular_centroide(rings)
        registros.append({
            "processo": processo,
            "lat": lat,
            "lon": lon,
            "geometry_raw": json.dumps(geometry, ensure_ascii=False),
            "dt_ingestao": hoje,
        })
    return registros


# ------------------------------------------------------------------
# Bronze genérico por seção (atributos OU geometria) — mesma garantia dos
# outros pipelines: checkpoint só avança depois do upload confirmado.
# ------------------------------------------------------------------
def ingest_secao_bronze(nome_secao: str, nome_arquivo: str, fetch_pagina,
                         modo: str, lote_paginas: int, page_size: int,
                         skip_upload: bool) -> pd.DataFrame:
    checkpoint = carregar_checkpoint()

    if modo == "overwrite":
        offset_inicial = 0
        bronze_df = pd.DataFrame()
    else:
        offset_inicial = checkpoint.get(nome_secao, {}).get("offset", 0)
        bronze_df = baixar_jsonl_do_blob(nome_arquivo, "bronze") if offset_inicial > 0 else pd.DataFrame()

    total = contar_total()
    log.info(f"{nome_secao}: {total:,} registros esperados"
             + (f" — retomando do offset {offset_inicial:,}" if offset_inicial else ""))

    if offset_inicial >= total and total > 0:
        log.info(f"{nome_secao}: checkpoint já cobre todos os {total:,} registros — nada a buscar")
        return bronze_df

    lote = []  # reseta a cada upload
    offset = offset_inicial

    while offset < total:
        try:
            pagina = fetch_pagina(offset, page_size)
        except Exception as e:
            log.error(f"{nome_secao}: falha ao buscar página no offset {offset} — "
                      f"{type(e).__name__}: {e}")
            traceback.print_exc()
            break

        if not pagina:
            break

        lote.extend(pagina)
        offset += len(pagina)
        log.info(f"{nome_secao}: {offset:,} / {total:,}")

        fechar_lote = (len(lote) >= lote_paginas * page_size) or offset >= total

        if fechar_lote and lote:
            combinado = pd.concat([bronze_df, pd.DataFrame(lote)], ignore_index=True) if not bronze_df.empty else pd.DataFrame(lote)

            if skip_upload:
                log.warning(f"{nome_secao}: --skip-upload ativo — checkpoint não avança "
                            f"(ficaria no offset {offset:,})")
                bronze_df = combinado  # mantém em memória pra silver funcionar nesta sessão
                lote = []
            else:
                sucesso = upload_dataframe_jsonl(combinado, nome_arquivo, subpasta="bronze")
                if sucesso:
                    bronze_df = combinado
                    checkpoint[nome_secao] = {"offset": offset, "total_no_momento": total}
                    salvar_checkpoint(checkpoint)
                    lote = []
                else:
                    # CRÍTICO: não pode continuar avançando o offset depois de um upload
                    # falhado — senão o checkpoint pularia direto pro offset seguinte (que
                    # SIM foi confirmado depois) e esse trecho no meio ficaria perdido pra
                    # sempre, sem nunca ter sido persistido e sem nunca ser retentado.
                    # Em vez disso, para a paginação aqui: o checkpoint fica no último
                    # offset confirmado, e a próxima execução em --modo append refaz esse
                    # trecho (e o que vier depois dele) do zero.
                    log.warning(f"{nome_secao}: upload falhou — parando aqui (offset "
                                f"confirmado permanece {checkpoint.get(nome_secao, {}).get('offset', 0):,}). "
                                f"Este trecho em diante será refeito na próxima execução em --modo append.")
                    bronze_df = combinado  # só pra esta sessão poder gerar a silver com o que deu
                    break

        time.sleep(DELAY_ENTRE_PAGINAS)

    log.info(f"{nome_secao}: {len(bronze_df):,} registros disponíveis ao final desta execução")
    return bronze_df


def _coletar_bronze_do_azure(nome_arquivo: str) -> pd.DataFrame:
    return baixar_jsonl_do_blob(nome_arquivo, "bronze")


# ------------------------------------------------------------------
# Silver — junta atributos + geometria, classifica, deduplica
# ------------------------------------------------------------------
_FASES_ATIVO = {"CONCESSÃO DE LAVRA", "LICENCIAMENTO"}
_FASES_EM_PROCESSO = {
    "REQUERIMENTO DE LAVRA", "REQUERIMENTO DE LICENCIAMENTO",
    "AUTORIZAÇÃO DE PESQUISA", "REQUERIMENTO DE PESQUISA",
    "DIREITO DE REQUERER A LAVRA",
}
_FASES_DISPONIVEL = {"APTO PARA DISPONIBILIDADE"}

_SUBS_ROCHA_BRITADA = {"BASALTO", "GRANITO", "QUARTZITO", "ARENITO", "MÁRMORE"}
_SUBS_CALCARIO = {"CALCÁRIO", "CALCÁRIO DOLOMÍTICO", "DOLOMITO"}
_SUBS_AREIA = {"AREIA", "AREIA INDUSTRIAL", "AREIA QUARTZOSA"}
_SUBS_CASCALHO = {"CASCALHO", "SAIBRO"}


def _classificar_fase_grupo(fase) -> str:
    if fase in _FASES_ATIVO:
        return "ativo"
    if fase in _FASES_EM_PROCESSO:
        return "em_processo"
    if fase in _FASES_DISPONIVEL:
        return "disponivel"
    return "outros"


def _classificar_tipo_agregado(substancia) -> str:
    if substancia in _SUBS_ROCHA_BRITADA:
        return "rocha_britada"
    if substancia in _SUBS_CALCARIO:
        return "calcario"
    if substancia in _SUBS_AREIA:
        return "areia"
    if substancia in _SUBS_CASCALHO:
        return "cascalho_saibro"
    return "outros"


def transformar_silver_sigmine(df_atributos: pd.DataFrame, df_geometria: pd.DataFrame) -> pd.DataFrame:
    silver = pd.DataFrame({
        "processo": df_atributos.get("PROCESSO"),
        "processo_formatado": df_atributos.get("DSProcesso"),
        "id_anm": df_atributos.get("ID"),
        "numero": pd.to_numeric(df_atributos.get("NUMERO"), errors="coerce").astype("Int64"),
        "ano": pd.to_numeric(df_atributos.get("ANO"), errors="coerce").astype("Int64"),
        "substancia": df_atributos.get("SUBS"),
        "uso": df_atributos.get("USO"),
        "fase": df_atributos.get("FASE"),
        "titular": df_atributos.get("NOME"),
        "area_ha": pd.to_numeric(df_atributos.get("AREA_HA"), errors="coerce"),
        "uf": df_atributos.get("UF"),
        "ultimo_evento": df_atributos.get("ULT_EVENTO"),
        "dt_ingestao": df_atributos.get("dt_ingestao"),
    })

    silver["fase_grupo"] = silver["fase"].apply(_classificar_fase_grupo)
    silver["tipo_agregado"] = silver["substancia"].apply(_classificar_tipo_agregado)

    silver = silver.drop_duplicates(subset=["processo"])

    if not df_geometria.empty:
        geo = df_geometria[["processo", "lat", "lon", "geometry_raw"]].drop_duplicates(subset=["processo"])
        silver = silver.merge(geo, on="processo", how="left")
    else:
        silver["lat"] = pd.NA
        silver["lon"] = pd.NA
        silver["geometry_raw"] = pd.NA

    return silver


def ingest_silver(df_atributos: pd.DataFrame, df_geometria: pd.DataFrame, skip_upload: bool = False) -> pd.DataFrame:
    silver = transformar_silver_sigmine(df_atributos, df_geometria)

    total = len(silver)
    com_geo = int(silver["lat"].notna().sum())
    sem_geo = total - com_geo
    log.info(f"{total:,} registros silver gerados em memória")
    if total:
        log.info(f"  Com coordenadas : {com_geo:,} ({com_geo / total * 100:.1f}%)")
        log.info(f"  Sem coordenadas : {sem_geo:,} ({sem_geo / total * 100:.1f}%)")

    if not skip_upload:
        upload_dataframe_parquet(silver, NOME_SILVER, subpasta="silver")
    else:
        log.info("--skip-upload ativo, silver não enviada")

    return silver


# ------------------------------------------------------------------
# CLI / main
# ------------------------------------------------------------------
def parse_args():
    parser = argparse.ArgumentParser(description="Ingestão SIGMINE/ANM (agregados SP) → bronze/silver no Azure")
    parser.add_argument("--modo", choices=["append", "overwrite"], default="overwrite",
                         help="overwrite (padrão): busca tudo do zero | append: retoma uma execução "
                              "anterior interrompida (ver docstring — não é acúmulo histórico)")
    parser.add_argument("--skip-ingest", action="store_true",
                         help="Não busca na API da ANM — baixa o bronze existente no Azure e só regenera a silver")
    parser.add_argument("--lote-paginas", type=int, default=5,
                         help="Quantas páginas acumular em memória antes de subir um lote para o Azure (padrão: 5)")
    parser.add_argument("--page-size", type=int, default=1000,
                         help="Registros por página na API (padrão: 1000)")
    parser.add_argument("--skip-upload", action="store_true",
                         help="Não sobe bronze/silver para o Azure. O checkpoint não avança — use só para testes.")
    return parser.parse_args()


def main():
    args = parse_args()

    log.info(f"Modo: {args.modo}")
    log.info(f"Lote: {args.lote_paginas} página(s) por upload | page-size: {args.page_size}")
    log.info(f"Upload Azure: {'desabilitado (--skip-upload)' if args.skip_upload else ('configurado' if _azure_configurado() else 'sem credenciais no .env')}")

    if args.skip_ingest:
        log.info("=== Bronze SIGMINE (via --skip-ingest, direto do Azure) ===")
        df_atributos = _coletar_bronze_do_azure(NOME_BRONZE_ATRIBUTOS)
        df_geometria = _coletar_bronze_do_azure(NOME_BRONZE_GEOMETRIA)
    else:
        log.info("=== Bronze SIGMINE — atributos ===")
        df_atributos = ingest_secao_bronze(
            "atributos", NOME_BRONZE_ATRIBUTOS, fetch_atributos_pagina,
            args.modo, args.lote_paginas, args.page_size, args.skip_upload,
        )
        log.info("=== Bronze SIGMINE — geometria ===")
        df_geometria = ingest_secao_bronze(
            "geometria", NOME_BRONZE_GEOMETRIA, fetch_geometria_pagina,
            args.modo, args.lote_paginas, args.page_size, args.skip_upload,
        )

    if df_atributos.empty:
        log.error("Nenhum dado de atributos disponível — nada a processar")
        return

    log.info("=== Silver SIGMINE ===")
    silver = ingest_silver(df_atributos, df_geometria, skip_upload=args.skip_upload)

    resumo = (
        silver.groupby(["tipo_agregado", "fase_grupo"])
              .agg(qtd_processos=("processo", "count"),
                   area_total_ha=("area_ha", lambda s: round(s.sum(), 0)))
              .reset_index()
              .sort_values(["tipo_agregado", "fase_grupo"])
    )
    log.info("Resumo por tipo de agregado / fase:\n" + resumo.to_string(index=False))


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log.warning("Interrompido manualmente (Ctrl+C). O checkpoint local só avança DEPOIS que um "
                    "lote de páginas é confirmado no Azure — rode de novo em --modo append para "
                    "retomar de onde parou (não do zero).")
    except Exception:
        log.error("O script encerrou por causa de um erro não previsto:")
        traceback.print_exc()
        raise