"""
pncp_extract.py

Versão adaptada do notebook Databricks (bronze -> silver) para rodar como
script Python puro em Linux, sem Spark/dbutils/Unity Catalog.

Saídas:
  - local:
      data/bronze_pncp_contratacoes.jsonl   (payload bruto, 1 JSON por linha)
      data/silver_pncp_contratacoes_obras.csv (tabela tratada, achatada)
  - Azure Blob Storage (opcional, ver .env):
      mesmos dois arquivos, subidos para {AZURE_STORAGE_CONTAINER}/{AZURE_BLOB_PREFIX}/

Dependências extras:
  pip install python-dotenv azure-storage-blob

Configuração (.env na raiz do projeto — NUNCA commitar esse arquivo):
  # opção A (mais simples)
  AZURE_STORAGE_CONNECTION_STRING=DefaultEndpointsProtocol=https;AccountName=...;AccountKey=...;EndpointSuffix=core.windows.net
  # opção B (alternativa à connection string)
  AZURE_STORAGE_ACCOUNT_NAME=minhaconta
  AZURE_STORAGE_ACCOUNT_KEY=xxxxx
  # comum às duas opções
  AZURE_STORAGE_CONTAINER=pncp
  AZURE_BLOB_PREFIX=obras/sp

Uso:
  python pncp_extract.py --anos 2022 2023 2024 --modo append
  python pncp_extract.py --anos 2024 --modo overwrite
  python pncp_extract.py --anos 2024 --skip-upload   # só local, ignora o .env
"""

import argparse
import calendar
import functools
import json
import os
import re
import threading
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, timedelta
from time import sleep

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from dotenv import load_dotenv

# SDK do Azure é opcional — o script roda normalmente (só local) mesmo sem
# ela instalada ou sem o .env configurado. Só quebra se você tentar de fato
# subir para o Azure sem os dois disponíveis.
try:
    from azure.storage.blob import BlobServiceClient
except ImportError:
    BlobServiceClient = None

# Garante que print() nunca fique preso em buffer — importante quando a saída
# é redirecionada para arquivo/log (ex: `python pncp_extract.py > log.txt`),
# senão as mensagens só aparecem no final (ou nunca, se o processo travar/morrer).
print = functools.partial(print, flush=True)

# Ancora os caminhos na localização do próprio arquivo pncp.py, não no
# diretório de onde o comando é executado (cwd). Isso evita que rodar o
# script de dentro de pncp/ crie um "data/" (ou procure um ".env") novo ali
# dentro, em vez de usar o que já existe um nível acima, na raiz do projeto.
BASE_DIR = os.path.dirname(os.path.abspath(__file__))       # .../pncp/

load_dotenv(os.path.join(BASE_DIR, "..", ".env"))  # lê o .env da raiz do projeto

# ── Config ───────────────────────────────────────────────────────────────
PNCP_BASE = "https://pncp.gov.br/api/consulta"
DELAY_ENTRE_REQUESTS = 1.5  # segundos entre chamadas, para não estourar rate limit
DATA_DIR = os.path.normpath(os.path.join(BASE_DIR, "..", "data"))  # .../data/
BRONZE_PATH = os.path.join(DATA_DIR, "bronze_pncp_contratacoes.jsonl")
SILVER_PATH = os.path.join(DATA_DIR, "silver_pncp_contratacoes_obras.csv")
CHECKPOINT_PATH = os.path.join(DATA_DIR, "checkpoint_pncp.json")

# ── Config Azure Blob Storage (tudo vem do .env, nunca hardcoded) ─────────
# Opção A (mais simples): AZURE_STORAGE_CONNECTION_STRING sozinha
# Opção B: AZURE_STORAGE_ACCOUNT_NAME + AZURE_STORAGE_ACCOUNT_KEY
# Em ambos os casos, AZURE_STORAGE_CONTAINER define o container de destino.
AZURE_CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
AZURE_ACCOUNT_NAME = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
AZURE_ACCOUNT_KEY = os.getenv("AZURE_STORAGE_ACCOUNT_KEY")
AZURE_CONTAINER = os.getenv("AZURE_STORAGE_CONTAINER", "pncp")
AZURE_BLOB_PREFIX = os.getenv("AZURE_BLOB_PREFIX_pncp", "").strip("/")


MODALIDADES_JANELA = {
    4: ("mensal", "Concorrência Eletrônica"),
    5: ("mensal", "Concorrência Presencial"),
    6: ("semanal", "Pregão Eletrônico"),
    8: ("mensal", "Dispensa de Licitação"),
}

PALAVRAS_OBRA = [
    "obra", "construção", "paviment", "reforma",
    "engenharia", "edificação", "saneamento", "drenagem",
    "ponte", "viaduto", "rodovia", "habitação",
]

_lock = threading.Lock()


# ── Checkpoint (janelas já processadas com sucesso) ─────────────────────
def _janela_key(modalidade_id: int, data_inicial: str, data_final: str) -> str:
    return f"{modalidade_id}:{data_inicial}:{data_final}"


def carregar_checkpoint() -> set:
    if not os.path.exists(CHECKPOINT_PATH):
        return set()
    with open(CHECKPOINT_PATH, "r", encoding="utf-8") as f:
        try:
            return set(json.load(f))
        except Exception:
            return set()


def salvar_checkpoint(chaves: set):
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(CHECKPOINT_PATH, "w", encoding="utf-8") as f:
        json.dump(sorted(chaves), f, ensure_ascii=False)


# ── Upload para Azure Blob Storage ────────────────────────────────────────
def _azure_configurado() -> bool:
    tem_connection_string = bool(AZURE_CONNECTION_STRING)
    tem_account_key = bool(AZURE_ACCOUNT_NAME and AZURE_ACCOUNT_KEY)
    return tem_connection_string or tem_account_key


def get_blob_service_client():
    """
    Retorna um BlobServiceClient configurado a partir do .env, ou None se a
    SDK não estiver instalada ou as credenciais não estiverem configuradas.
    Nunca levanta exceção — quem chama decide se aborta ou só avisa.
    """
    if BlobServiceClient is None:
        print("    ⚠️  Pacote 'azure-storage-blob' não instalado — pulando upload para o Azure. "
              "Instale com: pip install azure-storage-blob")
        return None

    if not _azure_configurado():
        print("    ⚠️  Nenhuma credencial do Azure encontrada no .env — pulando upload. "
              "Configure AZURE_STORAGE_CONNECTION_STRING (ou AZURE_STORAGE_ACCOUNT_NAME + "
              "AZURE_STORAGE_ACCOUNT_KEY) para habilitar.")
        return None

    try:
        if AZURE_CONNECTION_STRING:
            return BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)
        account_url = f"https://{AZURE_ACCOUNT_NAME}.blob.core.windows.net"
        return BlobServiceClient(account_url=account_url, credential=AZURE_ACCOUNT_KEY)
    except Exception as e:
        print(f"    ❌ Falha ao criar client do Azure Blob Storage: {type(e).__name__}: {e}")
        return None


def upload_para_blob(local_path: str, nome_arquivo: str) -> bool:
    """
    Sobe local_path para {AZURE_CONTAINER}/{AZURE_BLOB_PREFIX}/{nome_arquivo}.
    Sempre sobrescreve o blob (o arquivo local já reflete o estado atual,
    seja modo append ou overwrite). Retorna True/False — nunca derruba o
    pipeline principal se o upload falhar (o CSV/JSONL local já está salvo).
    """
    client = get_blob_service_client()
    if client is None:
        return False

    if not os.path.exists(local_path):
        print(f"    ⚠️  Arquivo local não encontrado para upload: {local_path}")
        return False

    blob_name = f"{AZURE_BLOB_PREFIX}/{nome_arquivo}" if AZURE_BLOB_PREFIX else nome_arquivo

    try:
        container_client = client.get_container_client(AZURE_CONTAINER)
        if not container_client.exists():
            container_client.create_container()
            print(f"    ✓ Container '{AZURE_CONTAINER}' criado no Azure")

        with open(local_path, "rb") as f:
            container_client.upload_blob(name=blob_name, data=f, overwrite=True)

        tamanho_mb = os.path.getsize(local_path) / (1024 * 1024)
        print(f"    ✓ Upload OK → container '{AZURE_CONTAINER}' / blob '{blob_name}' ({tamanho_mb:.2f} MB)")
        return True
    except Exception as e:
        print(f"    ❌ Falha no upload para o Azure ({nome_arquivo}): {type(e).__name__}: {e}")
        return False


# ── HTTP session com retry automático ───────────────────────────────────
def criar_session() -> requests.Session:
    session = requests.Session()
    # 429 NÃO entra aqui de propósito — é tratado manualmente em fetch_janela,
    # com log visível a cada tentativa. Deixar o urllib3 lidar com 429 internamente
    # faz a chamada bloquear em silêncio por minutos (parece que o script travou).
    retry = Retry(
        total=3,
        backoff_factor=2,
        status_forcelist=[500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    session.mount("https://", HTTPAdapter(max_retries=retry))
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "application/json",
    })
    return session


# ── Janelas de data ──────────────────────────────────────────────────────
def _janelas_mensais(anos):
    janelas = []
    for ano in anos:
        for mes in range(1, 13):
            ultimo = calendar.monthrange(ano, mes)[1]
            janelas.append((f"{ano}{mes:02d}01", f"{ano}{mes:02d}{ultimo:02d}"))
    return janelas


def _janelas_semanais(anos):
    janelas = []
    for ano in anos:
        inicio = date(ano, 1, 1)
        while inicio.year == ano:
            fim = min(inicio + timedelta(days=6), date(ano, 12, 31))
            janelas.append((inicio.strftime("%Y%m%d"), fim.strftime("%Y%m%d")))
            inicio = fim + timedelta(days=1)
    return janelas


# ── Fetch de uma janela completa ─────────────────────────────────────────
def fetch_janela(modalidade_id: int, modalidade_nome: str,
                  data_inicial: str, data_final: str) -> dict:
    """
    Retorna um dict com:
      - obras: lista de registros que bateram com PALAVRAS_OBRA
      - total_avaliado: quantos contratos a API de fato devolveu (antes do filtro)
      - eventos: lista de strings descrevendo qualquer status/erro anômalo
    Isso permite diferenciar "zero obras porque a API não tinha nada com essas
    palavras" de "zero obras porque a chamada falhou e nunca foi avaliada".
    """
    session = criar_session()  # session própria por thread
    resultado = []
    total_avaliado = 0
    eventos = []
    pagina = 1
    hoje = str(date.today())

    while True:
        resp = None
        tentativas_429 = 0
        while True:  # loop de tentativas para essa página (timeout/conexão/429)
            print(f"    … buscando {data_inicial[:6]} mod={modalidade_id} pág={pagina}", end="\r")
            try:
                resp = session.get(
                    f"{PNCP_BASE}/v1/contratacoes/publicacao",
                    params={
                        "dataInicial": data_inicial,
                        "dataFinal": data_final,
                        "uf": "SP",
                        "codigoModalidadeContratacao": modalidade_id,
                        "pagina": pagina,
                        "tamanhoPagina": 50,
                    },
                    timeout=45,
                )
            except (requests.exceptions.Timeout,
                    requests.exceptions.ConnectionError) as e:
                msg = f"conexão falhou (pág={pagina}): {type(e).__name__}"
                eventos.append(msg)
                print(f"    ❌ {data_inicial[:6]} mod={modalidade_id} pág={pagina} — desistindo ({msg})")
                return {"obras": resultado, "total_avaliado": total_avaliado, "eventos": eventos}

            if resp.status_code == 429:
                tentativas_429 += 1
                if tentativas_429 > 5:
                    msg = f"429 persistente após 5 tentativas (pág={pagina})"
                    eventos.append(msg)
                    print(f"    ❌ {data_inicial[:6]} mod={modalidade_id} — {msg}, desistindo dessa janela")
                    return {"obras": resultado, "total_avaliado": total_avaliado, "eventos": eventos}
                # respeita Retry-After se o servidor mandar; senão, backoff crescente
                espera = resp.headers.get("Retry-After")
                espera = float(espera) if espera and espera.isdigit() else 15 * tentativas_429
                print(f"    ⏳ {data_inicial[:6]} mod={modalidade_id} pág={pagina} — 429, "
                      f"aguardando {espera:.0f}s (tentativa {tentativas_429}/5)")
                sleep(espera)
                continue  # tenta a mesma página de novo

            break  # status != 429 (sucesso, 204, erro real etc.) — sai do loop de tentativas

        if resp.status_code == 204:
            break

        # status inesperado (400/403/5xx residual etc.)
        if resp.status_code != 200:
            msg = f"HTTP {resp.status_code} (pág={pagina}): {resp.text[:200]!r}"
            eventos.append(msg)
            print(f"    ⚠️  {data_inicial[:6]} mod={modalidade_id} — {msg}")
            break

        try:
            data = resp.json()
        except Exception as e:
            msg = f"JSON inválido (pág={pagina}): {type(e).__name__}: {e}"
            eventos.append(msg)
            print(f"    ⚠️  {data_inicial[:6]} mod={modalidade_id} — {msg}")
            break

        batch = data.get("data", [])
        if not batch:
            break

        total_avaliado += len(batch)
        for item in batch:
            objeto = (item.get("objetoCompra") or "").lower()
            if any(p in objeto for p in PALAVRAS_OBRA):
                resultado.append({
                    "payload": json.dumps(item, ensure_ascii=False),
                    "fonte": "pncp",
                    "modalidade": modalidade_nome,
                    "ano_referencia": data_inicial[:4],
                    "dt_ingestao": hoje,
                })

        if pagina >= data.get("totalPaginas", 1):
            break
        pagina += 1
        sleep(DELAY_ENTRE_REQUESTS)

    return {"obras": resultado, "total_avaliado": total_avaliado, "eventos": eventos}


# ── Ingestão bronze ──────────────────────────────────────────────────────
def ingest_pncp_bronze(anos: list, modo: str, skip_upload: bool = False) -> list:
    """
    modo:
      - 'overwrite': apaga o bronze existente e recomeça do zero
      - 'append'   : mantém o bronze existente e adiciona os novos registros

    skip_upload: se False (padrão), sobe o bronze acumulado para o Azure ao
    final de CADA modalidade (ex: ao terminar Pregão Eletrônico), não só no
    final da ingestão inteira. Assim, mesmo que o script seja interrompido
    numa modalidade posterior, o Azure já tem tudo que foi concluído até ali.
    """
    os.makedirs(DATA_DIR, exist_ok=True)

    if modo == "overwrite" and os.path.exists(BRONZE_PATH):
        os.remove(BRONZE_PATH)
    if modo == "overwrite" and os.path.exists(CHECKPOINT_PATH):
        os.remove(CHECKPOINT_PATH)

    checkpoint = carregar_checkpoint()
    print(f"Checkpoint: {len(checkpoint):,} janela(s) já concluída(s) em execuções anteriores (serão puladas)")

    registros = []
    resumo_modalidades = []

    for modalidade_id, (tipo_janela, modalidade_nome) in MODALIDADES_JANELA.items():
        todas_janelas = _janelas_semanais(anos) if tipo_janela == "semanal" else _janelas_mensais(anos)
        janelas = [
            (di, df) for di, df in todas_janelas
            if _janela_key(modalidade_id, di, df) not in checkpoint
        ]
        puladas = len(todas_janelas) - len(janelas)
        workers = 1  # ajuste se quiser paralelizar (mesma ressalva do notebook original)

        print(f"\n  → {modalidade_nome} | {len(janelas)} janelas a processar"
              + (f" ({puladas} já concluídas via checkpoint)" if puladas else "")
              + f" | {workers} worker(s)")

        total_avaliado_mod = 0
        total_obras_mod = 0
        eventos_mod = []
        janelas_com_falha = 0

        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {
                executor.submit(fetch_janela, modalidade_id, modalidade_nome, di, df): (di, df)
                for di, df in janelas
            }
            for future in as_completed(futures):
                di, df_ = futures[future]
                try:
                    res = future.result()
                    lote = res["obras"]
                    total_avaliado_mod += res["total_avaliado"]
                    total_obras_mod += len(lote)
                    if res["eventos"]:
                        janelas_com_falha += 1
                        eventos_mod.extend(f"{di[:6]}: {ev}" for ev in res["eventos"])
                    else:
                        # só marca como concluída no checkpoint se não houve nenhum evento
                        # de erro — assim ela é reprocessada automaticamente na próxima vez
                        with _lock:
                            checkpoint.add(_janela_key(modalidade_id, di, df_))
                            salvar_checkpoint(checkpoint)
                    if lote:
                        with _lock:
                            registros.extend(lote)
                            # grava incrementalmente no bronze (jsonl) — evita perder
                            # progresso em caso de queda no meio da execução
                            with open(BRONZE_PATH, "a", encoding="utf-8") as f:
                                for reg in lote:
                                    f.write(json.dumps(reg, ensure_ascii=False) + "\n")
                        print(f"    ✓ {di[:6]} — {len(lote)} obras (de {res['total_avaliado']} contratos avaliados)")
                except Exception as e:
                    janelas_com_falha += 1
                    eventos_mod.append(f"{di[:6]}: exceção não tratada {type(e).__name__}: {e}")
                    print(f"    ❌ {di[:6]} — exceção não tratada:")
                    traceback.print_exc()

        resumo_modalidades.append({
            "modalidade": modalidade_nome,
            "janelas": len(janelas),
            "janelas_puladas": puladas,
            "janelas_com_evento": janelas_com_falha,
            "contratos_avaliados": total_avaliado_mod,
            "obras_encontradas": total_obras_mod,
        })

        if eventos_mod:
            print(f"    ⚠️  {len(eventos_mod)} evento(s) anômalo(s) em '{modalidade_nome}' "
                  f"(status inesperado / JSON inválido / falha de conexão):")
            for ev in eventos_mod[:10]:
                print(f"       - {ev}")
            if len(eventos_mod) > 10:
                print(f"       ... e mais {len(eventos_mod) - 10}")

        # upload incremental: sobe o bronze acumulado até aqui assim que essa
        # modalidade termina, em vez de esperar a ingestão inteira acabar
        if not skip_upload and os.path.exists(BRONZE_PATH):
            print(f"    ↑ Subindo bronze para o Azure (checkpoint: fim de '{modalidade_nome}')...")
            upload_para_blob(BRONZE_PATH, os.path.basename(BRONZE_PATH))

    print("\n=== Diagnóstico por modalidade ===")
    for r in resumo_modalidades:
        print(f"  {r['modalidade']:<28} janelas={r['janelas']:>4}  "
              f"puladas(checkpoint)={r['janelas_puladas']:>4}  "
              f"com_evento={r['janelas_com_evento']:>3}  "
              f"contratos_avaliados={r['contratos_avaliados']:>6}  "
              f"obras_encontradas={r['obras_encontradas']:>4}")
        if r["contratos_avaliados"] == 0 and r["janelas_com_evento"] == 0:
            print(f"    ⚠️  Nenhum contrato foi avaliado e nenhum evento foi registrado — "
                  f"a API pode estar retornando 204 (sem conteúdo) genuinamente para essa "
                  f"modalidade/período, mas vale checar manualmente 1 janela no navegador.")
        elif r["contratos_avaliados"] == 0 and r["janelas_com_evento"] > 0:
            print(f"    ❌ Todas as janelas com evento e ZERO contratos avaliados — "
                  f"forte indício de que as chamadas para essa modalidade estão falhando "
                  f"(ver eventos acima), não que não existem obras.")

    print(f"\nTotal coletado nesta execução: {len(registros):,} registros")
    print(f"✓ Bronze salvo em → {BRONZE_PATH}")
    return registros


# ── Transformação silver ─────────────────────────────────────────────────
def _classificar_tipo_obra(objeto: str) -> str:
    objeto = objeto or ""
    padroes = [
        (r"(?i)paviment|asfalto|calçad", "pavimentação"),
        (r"(?i)saneamento|esgoto|água|abastecimento", "saneamento"),
        (r"(?i)escola|creche|ub[sc]|saúde|hospital|cras|creas", "equipamento social"),
        (r"(?i)ponte|viaduto|rodovia|estrada|túnel", "viário"),
        (r"(?i)habitação|moradia|conjunto residencial", "habitação"),
        (r"(?i)drenagem|córrego|galeria", "drenagem"),
        (r"(?i)praça|parque|quadra|arena|ginásio", "equipamento esportivo/lazer"),
    ]
    for padrao, categoria in padroes:
        if re.search(padrao, objeto):
            return categoria
    return "construção geral"


def transform_silver_contratacoes() -> pd.DataFrame:
    """
    Lê o bronze (jsonl com payload bruto), achata o JSON aninhado e produz
    a tabela silver equivalente à do notebook original.
    """
    if not os.path.exists(BRONZE_PATH):
        raise FileNotFoundError(f"Bronze não encontrado em {BRONZE_PATH}. Rode a ingestão primeiro.")

    linhas_bronze = []
    with open(BRONZE_PATH, "r", encoding="utf-8") as f:
        for linha in f:
            linha = linha.strip()
            if linha:
                linhas_bronze.append(json.loads(linha))

    registros = []
    for linha in linhas_bronze:
        d = json.loads(linha["payload"])
        orgao = d.get("orgaoEntidade") or {}
        unidade = d.get("unidadeOrgao") or {}
        amparo = d.get("amparoLegal") or {}

        registros.append({
            "fonte": linha.get("fonte"),
            "modalidade": linha.get("modalidade"),
            "ano_referencia": linha.get("ano_referencia"),
            "dt_ingestao": linha.get("dt_ingestao"),
            "numero_controle_pncp": d.get("numeroControlePNCP"),
            "numero_compra": d.get("numeroCompra"),
            "numero_processo": d.get("processo"),
            "ano_compra": d.get("anoCompra"),
            "sequencial_compra": d.get("sequencialCompra"),
            "objeto_compra": d.get("objetoCompra"),
            "modalidade_id": d.get("modalidadeId"),
            "modalidade_nome": d.get("modalidadeNome"),
            "modo_disputa_id": d.get("modoDisputaId"),
            "modo_disputa_nome": d.get("modoDisputaNome"),
            "sistema_registro_preco": d.get("srp"),
            "situacao_id": d.get("situacaoCompraId"),
            "situacao_nome": d.get("situacaoCompraNome"),
            "valor_estimado": d.get("valorTotalEstimado"),
            "valor_homologado": d.get("valorTotalHomologado"),
            "dt_publicacao_pncp": d.get("dataPublicacaoPncp"),
            "dt_inclusao": d.get("dataInclusao"),
            "dt_atualizacao": d.get("dataAtualizacao"),
            "dt_abertura_proposta": d.get("dataAberturaProposta"),
            "dt_encerramento_proposta": d.get("dataEncerramentoProposta"),
            "orgao_cnpj": orgao.get("cnpj"),
            "orgao_razao_social": orgao.get("razaoSocial"),
            "orgao_poder_id": orgao.get("poderId"),
            "orgao_esfera_id": orgao.get("esferaId"),
            "unidade_codigo": unidade.get("codigoUnidade"),
            "unidade_nome": unidade.get("nomeUnidade"),
            "municipio_ibge": unidade.get("codigoIbge"),
            "municipio_nome": unidade.get("municipioNome"),
            "uf": unidade.get("ufSigla"),
            "amparo_legal_codigo": amparo.get("codigo"),
            "amparo_legal_nome": amparo.get("nome"),
            "link_sistema_origem": d.get("linkSistemaOrigem"),
            "sistema_origem": d.get("usuarioNome"),
        })

    silver = pd.DataFrame(registros)

    # datas: converte para datetime (equivalente ao to_timestamp do Spark)
    for col in ["dt_publicacao_pncp", "dt_inclusao", "dt_atualizacao",
                "dt_abertura_proposta", "dt_encerramento_proposta"]:
        silver[col] = pd.to_datetime(silver[col], errors="coerce")

    silver["tipo_obra"] = silver["objeto_compra"].apply(_classificar_tipo_obra)

    silver = silver.drop_duplicates(subset=["numero_controle_pncp"])

    os.makedirs(DATA_DIR, exist_ok=True)
    silver.to_csv(SILVER_PATH, index=False, encoding="utf-8-sig")

    print(f"✓ {len(silver):,} registros → {SILVER_PATH}")
    return silver


# ── Main ──────────────────────────────────────────────────────────────────
def main():
    global DELAY_ENTRE_REQUESTS

    parser = argparse.ArgumentParser(description="Extração PNCP (obras) → CSV local")
    parser.add_argument("--anos", nargs="+", type=int, default=[2022, 2023, 2024],
                         help="Anos a coletar, ex: --anos 2022 2023 2024")
    parser.add_argument("--modo", choices=["append", "overwrite"], default="append",
                         help="append: mantém bronze/checkpoint existentes | overwrite: recomeça do zero")
    parser.add_argument("--skip-ingest", action="store_true",
                         help="Pula a ingestão e só reprocessa o bronze existente para silver")
    parser.add_argument("--delay", type=float, default=DELAY_ENTRE_REQUESTS,
                         help=f"Segundos de espera entre chamadas à API (padrão: {DELAY_ENTRE_REQUESTS}). "
                              f"Aumente se continuar tomando 429.")
    parser.add_argument("--skip-upload", action="store_true",
                         help="Não sobe bronze/silver para o Azure Blob Storage, mesmo se o .env estiver configurado")
    args = parser.parse_args()

    DELAY_ENTRE_REQUESTS = args.delay

    print(f"Anos:  {args.anos}")
    print(f"Modo:  {args.modo}")
    print(f"Delay: {DELAY_ENTRE_REQUESTS}s entre chamadas")
    print(f"Upload Azure: {'desabilitado (--skip-upload)' if args.skip_upload else ('configurado' if _azure_configurado() else 'sem credenciais no .env')}")

    if not args.skip_ingest:
        print("=== Bronze PNCP ===")
        ingest_pncp_bronze(args.anos, args.modo, skip_upload=args.skip_upload)
        # upload do bronze já acontece dentro de ingest_pncp_bronze, ao final
        # de cada modalidade — não precisa subir de novo aqui.

    print("\n=== Silver PNCP (obras) ===")
    silver = transform_silver_contratacoes()

    if not args.skip_upload:
        print("\n=== Upload silver → Azure Blob Storage ===")
        upload_para_blob(SILVER_PATH, os.path.basename(SILVER_PATH))

    # resumo equivalente ao display() do notebook
    resumo = (
        silver.groupby(["tipo_obra", "orgao_esfera_id"])
              .agg(qtd=("numero_controle_pncp", "count"),
                   valor_total_mi=("valor_estimado", lambda s: round(s.sum() / 1e6, 2)))
              .reset_index()
              .sort_values("qtd", ascending=False)
    )
    print("\nResumo por tipo de obra / esfera:")
    print(resumo.to_string(index=False))


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⏹️  Interrompido manualmente (Ctrl+C). O progresso já salvo no bronze/checkpoint "
              "não foi perdido — rode de novo em modo append para continuar de onde parou.")
    except Exception:
        print("\n\n💥 O script encerrou por causa de um erro não previsto:")
        traceback.print_exc()
        raise