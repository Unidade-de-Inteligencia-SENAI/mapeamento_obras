# sissel.py — Ingestão Bronze/Silver SISSEL (Alvarás SMUL)

Pipeline em **Python puro** que baixa as bases anuais de Alvarás do SISSEL (SMUL, Prefeitura de SP) e salva tudo no **Azure Blob Storage**. 

## Visão geral do fluxo

```
SISSEL (1 arquivo XLS/XLSX por ano)
      │
      ▼
gerar_sissel_urls()  ──►  URLs fixas (2000-2023, hardcoded) + URLs recentes
      │                   (2024+, via scraping da página do portal SMUL)
      ▼
ingest_sissel_bronze()  ──►  baixa cada ano pendente, sobe CRU pro Azure
      │                       <container>/<prefixo>/bronze/<ano>.xls(x)
      ▼
montar_raw()  ──►  lê todos os anos (bronze recém-baixado + reaproveitado
      │            do Azure), detecta aba/header, normaliza nomes de coluna
      │            (mantendo acentos), concatena tudo
      ▼
transformar_silver_sissel()  ──►  unifica nomes de coluna que mudaram entre
      │                            anos (schema evolution), limpa área/data,
      │                            classifica uso, deduplica
      ▼
<container>/<prefixo>/silver/sissel_alvaras.parquet
```

## Regra especial: o ano corrente sempre é rebaixado

O arquivo do SISSEL do ano corrente **acumula meses ao longo do ano** — o arquivo baixado em janeiro fica desatualizado em fevereiro. Por isso, mesmo que o ano corrente já esteja no checkpoint, ele é **sempre rebaixado** em toda execução. Anos fechados (anteriores ao corrente) só são rebaixados se ainda não estiverem no checkpoint, ou em `--modo overwrite`.

Isso está testado explicitamente: numa execução em `--modo append`, anos antigos já concluídos não geram nenhuma chamada de download nova; só o ano corrente é buscado de novo, mesmo estando no checkpoint.

## Descoberta de URLs (isso é scraping de verdade)

Diferente do download dos arquivos em si (que é um download direto, não scraping), a descoberta de **quais URLs existem para os anos recentes** é feita fazendo parsing de HTML da página do portal SMUL — isso sim é scraping, com todas as fragilidades que isso implica (quebra se o layout da página mudar).

- **2000–2023**: URLs fixas, hardcoded em `SISSEL_URLS_FIXAS` — não mudam, não dependem de scraping.
- **2024+**: extraídas dinamicamente de `https://prefeitura.sp.gov.br/licenciamento/w/servicos/3334`, procurando links no padrão `sissel_ano_YYYY[_MM]`. Quando há mais de um arquivo pro mesmo ano (ex: `sissel_ano_2026_02`, `sissel_ano_2026_03`), o script pega o **mês mais recente** automaticamente.

**Resiliente por padrão**: se o portal estiver fora do ar (3 tentativas com espera crescente) ou o layout mudar (nenhum link no padrão esperado for encontrado), o script **não trava** — só loga um aviso e segue com as URLs fixas (2000-2023). Isso significa que, na pior hipótese, você sempre consegue reprocessar o histórico, mesmo que a descoberta de anos novos esteja temporariamente quebrada.

## Uso (linha de comando)

```bash
python sissel.py                              # todos os anos cadastrados (fixos + descobertos via scraping)
python sissel.py --anos 2023 2024 2025        # só um subconjunto de anos
python sissel.py --modo overwrite              # ignora checkpoint, rebaixa tudo
python sissel.py --skip-ingest                 # só regenera a silver a partir do bronze existente no Azure
python sissel.py --skip-upload                 # teste pontual — checkpoint não avança
python sissel.py --help
```

### Todos os parâmetros

| Flag | Padrão | Descrição |
|---|---|---|
| `--anos` | todos os anos em `SISSEL_URLS_ANUAIS` (fixos + descobertos) | Anos a processar. Ex: `--anos 2023 2024 2025` |
| `--modo` | `append` | `append` pula anos já concluídos no checkpoint (exceto o ano corrente, sempre rebaixado) \| `overwrite` reprocessa tudo do zero |
| `--skip-ingest` | desligado | Não baixa da fonte SISSEL — baixa o bronze existente no Azure para todos os anos pedidos e só regenera a silver |
| `--skip-upload` | desligado | Não sobe bronze/silver para o Azure. O checkpoint não avança — use só para testes pontuais |

## Configuração (`.env`)

O `.env` fica na **raiz do projeto**, um nível acima da pasta deste script (`BASE_DIR/../.env`), mesmo padrão dos outros três pipelines. Não versionar.

| Variável | Obrigatória? | Padrão | Descrição |
|---|---|---|---|
| `AZURE_STORAGE_CONNECTION_STRING` | Uma das duas opções de credencial | — | Connection string completa (opção mais simples) |
| `AZURE_STORAGE_ACCOUNT_NAME` + `AZURE_STORAGE_ACCOUNT_KEY` | Alternativa à connection string | — | Autenticação por conta + chave |
| `AZURE_STORAGE_CONTAINER` | Não | `conteiner` | Container onde bronze e silver são gravados |
| `AZURE_BLOB_PREFIX_sissel` | Não | `sissel` | Prefixo do caminho — vira `sissel/bronze/...` e `sissel/silver/...` |
| `CHECKPOINT_PATH` | Não | `data/checkpoint_sissel.json` (um nível acima da pasta do script) | Caminho do checkpoint |

## Dependências

```bash
pip install pandas numpy pyarrow requests python-dotenv azure-storage-blob openpyxl xlrd beautifulsoup4
```

- `openpyxl` lê os anos em `.xlsx` (formato novo, mais recente).
- `xlrd` lê os anos em `.xls` (formato antigo do Excel, usado pela maioria dos anos até ~2023).
- `beautifulsoup4` é usado só na descoberta de URLs recentes (scraping do portal).

Se faltar `xlrd` ou `openpyxl`, a leitura daquele(s) ano(s) específico(s) falha com um erro claro no log (`ImportError: Install xlrd...`), mas **não derruba os outros anos** — cada ano é lido de forma independente, então um erro de dependência falta só marca aquele ano como erro na `montar_raw()`, sem afetar o resto.

## Camada Bronze

- Baixa o XLS/XLSX de cada ano. Falha de conexão tenta de novo até 3 vezes, com espera crescente (10s, 20s).
- Sobe o arquivo **exatamente como veio**, sem nenhuma transformação, para `bronze/<ano>.xls(x)`.
- Extensão detectada pelo sufixo da URL (`-xlsx`/`.xlsx` → `.xlsx`; qualquer outra coisa → `.xls`).
- Um ano só entra no checkpoint depois que o upload do bronze é confirmado no Azure — nunca antes.

## Camada Silver

Duas etapas, ambas sempre reprocessando **todos os anos disponíveis** a cada execução (não é incremental — não existe checkpoint próprio para a silver):

### 1. `montar_raw()` — leitura e normalização básica por ano

- Escolhe automaticamente a **aba com mais linhas** dentro do arquivo Excel (algumas planilhas têm abas extras vazias ou com metadados).
- Detecta a **linha do cabeçalho real** procurando por palavras-chave (`mês`, `alvará`, `unidade`, `bairro`, `descrição`...) — necessário porque algumas planilhas têm linhas de título antes do cabeçalho de verdade.
- Normaliza nomes de coluna (minúsculo, espaço/parênteses/`%`/`.`/quebra de linha viram `_`, sem repetição de `_`) — mas **preserva acentos**, porque a unificação de variantes acentuadas/não-acentuadas (`mês` vs `meses`) acontece só na próxima etapa.
- Adiciona `ano_referencia` e `dt_ingestao`.
- Concatena todos os anos processáveis num único DataFrame.

### 2. `transformar_silver_sissel()` — unificação de schema entre anos

O SISSEL mudou os nomes de várias colunas ao longo dos anos (`mês` → `meses`, `alvará` → `alvara`, `administração_regional` → `subprefeitura`, etc.). Esta etapa:

- Usa `_coalesce()` (equivalente ao `F.coalesce` do Spark) para pegar o primeiro valor não-nulo entre as variantes de nome conhecidas, coluna por coluna.
- Limpa área de construção/terreno: remove separador de milhar (`.`), troca decimal `,`→`.`, converte para número.
- Parseia datas tentando `dd/MM/yyyy` primeiro, depois `dd/MM/yy` nas que sobraram.
- Junta blocos/pavimentos/unidades (`_concat_ws()`, equivalente ao `F.concat_ws` — pula valores nulos sem deixar separador sobrando).
- Classifica `uso_classificado` por regex na categoria de uso (`R1-4`/`resid`/`HIS`/`HMP` → residencial; `C1-4`/`comerc` → comercial; `I1-4`/`indust` → industrial; `E1-3`/`equip` → equipamento; resto → outros).
- Remove duplicatas por `(alvara, processo, ano_referencia)`.

## Checkpoint local

Um único arquivo (`data/checkpoint_sissel.json`), gravado de forma atômica:

```json
{
  "anos_concluidos": ["2020", "2021", "2022", "2023"],
  "atualizado_em": "2026-08-19T13:59:27"
}
```

- `--modo append` (padrão): pula anos já no checkpoint, **exceto o ano corrente**.
- `--modo overwrite`: apaga o checkpoint e rebaixa todos os anos pedidos.
- Um ano só entra aqui depois que o **upload do bronze** é confirmado no Azure — se falhar, não entra, e é retentado automaticamente na próxima execução em `--modo append`.

## Tratamento de erros

- **`Ctrl+C`**: mensagem explicando que o checkpoint só marca um ano como concluído depois do upload confirmado — o que não deu tempo de subir será refeito na próxima execução, sem stack trace no log.
- **Erro não previsto**: imprime o traceback completo e **relança a exceção** (`raise`), garantindo exit code ≠ 0 — importante para agendadores (cron) detectarem falha.
- **Falha isolada por ano** (download, leitura): não derruba os outros anos da mesma execução — cada ano é processado de forma independente na leitura; só o(s) ano(s) com erro fica(m) de fora do DataFrame final, listado(s) no log.

## Estrutura de caminhos no Azure

```
<container>/
└── <prefixo (AZURE_BLOB_PREFIX, padrão "sissel")>/
    ├── bronze/
    │   ├── 2000.xls
    │   ├── 2001.xls
    │   ├── ...
    │   ├── 2024.xlsx
    │   └── 2026.xls
    └── silver/
        └── sissel_alvaras.parquet
```

## Logging

- Todos os logs vão para `stdout` no formato `timestamp | LEVEL | mensagem`.
- Os loggers internos do SDK do Azure são elevados para `WARNING`, evitando poluição com detalhes de request/response HTTP.
- Na inicialização, o script loga onde encontrou o `.env`, o container e o prefixo efetivo.
- Durante a descoberta de URLs, loga cada ano/mês encontrado no portal.
- Durante a leitura de cada ano, loga qual aba foi escolhida e quantas linhas ela tem.

## Problemas comuns

**`ImportError: Import xlrd failed` / `Import openpyxl failed`**
Faltam as bibliotecas de leitura de Excel:
```bash
pip install xlrd openpyxl
```
`xlrd` cobre os anos em `.xls` (a maioria, até ~2023); `openpyxl` cobre os anos em `.xlsx` (formato mais recente). O erro não derruba os outros anos — só aquele(s) ano(s) específico(s) falha(m) e fica(m) de fora do silver, listados no log. Depois de instalar, roda de novo em `--modo append`: o bronze que já tinha subido é reaproveitado, só a leitura é refeita.

**Nenhum ano recente (2024+) aparece, só os fixos**
Confira se o portal `https://prefeitura.sp.gov.br/licenciamento/w/servicos/3334` está no ar e se o padrão de link ainda é `sissel_ano_YYYY[_MM]` — se o SMUL mudou o layout da página, o scraping para de encontrar links e cai no fallback (só URLs fixas), com um aviso no log (`Nenhum link 'sissel_ano_*' encontrado`). Nesse caso é preciso ajustar o regex em `_scrape_urls_recentes()`.

**Um ano específico sempre falha na leitura, mesmo com as libs instaladas**
Pode ser que a heurística de detecção de header (`PALAVRAS_HEADER`) não encontre 2 das palavras-chave nessa planilha específica (formato mais antigo/diferente do esperado) — nesse caso ela cai pro header na linha 0, o que pode gerar colunas erradas. Vale inspecionar manualmente esse ano e, se necessário, ajustar `PALAVRAS_HEADER` ou tratar esse ano como caso especial.

**Quero rodar em background com log salvo em arquivo**
```bash
python -u sissel.py > log.txt 2>&1 &
```