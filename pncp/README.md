# pncp_extract.py

Script Python puro (sem Spark/Databricks) que extrai contratações de **obras**
do Portal Nacional de Contratações Públicas (PNCP) para o estado de SP,
transforma em uma tabela tratada e salva localmente em CSV — com upload
opcional para o Azure Blob Storage.

## O que ele faz

1. **Bronze** — consulta a API pública do PNCP (`/v1/contratacoes/publicacao`)
   para 4 modalidades de contratação (Concorrência Eletrônica, Concorrência
   Presencial, Pregão Eletrônico, Dispensa de Licitação), varrendo janelas de
   data (mensais ou semanais, dependendo do volume da modalidade) para os
   anos pedidos. Filtra apenas contratos cujo objeto contenha palavras
   relacionadas a obras (`obra`, `construção`, `paviment`, `reforma`,
   `saneamento`, `drenagem`, `ponte`, `habitação` etc.) e salva o payload
   bruto em `data/bronze_pncp_contratacoes.jsonl` (um JSON por linha).

2. **Silver** — lê o bronze, achata os campos aninhados (órgão, unidade,
   amparo legal), classifica cada contrato num `tipo_obra` (pavimentação,
   saneamento, viário, habitação, drenagem, equipamento social/esportivo,
   construção geral) via regex no objeto da compra, remove duplicados e
   salva em `data/silver_pncp_contratacoes_obras.csv`.

3. **Upload para o Azure** — se o `.env` estiver configurado, o
   bronze é enviado para o Azure Blob Storage ao final de **cada modalidade**
   concluída (não só no fim do processo inteiro), e o silver é enviado ao
   final da transformação.

## Pré-requisitos

- Python 3.9+
- Dependências (arquivo `requirements.txt`):

```bash
pip install -r requirements.txt
```

Isso instala `requests`, `pandas`, `python-dotenv` e `azure-storage-blob`.
Se você não vai usar o upload para o Azure, pode pular a instalação do
`azure-storage-blob` — o script detecta que o pacote não está presente e
simplesmente pula essa etapa com um aviso, sem quebrar.

## Configuração do Azure (opcional)

1. Copie `.env.example` para `.env`:

```bash
cp .env.example .env
```

2. Preencha o `.env` com as credenciais da Storage Account (veja a seção
   "Onde encontrar as credenciais do Azure" abaixo). Só é preciso preencher
   **uma** das duas opções:

```ini
# Opção A — mais simples
AZURE_STORAGE_CONNECTION_STRING=DefaultEndpointsProtocol=https;AccountName=...;AccountKey=...;EndpointSuffix=core.windows.net

# Opção B — alternativa à connection string
AZURE_STORAGE_ACCOUNT_NAME=minhaconta
AZURE_STORAGE_ACCOUNT_KEY=xxxxx

# comum às duas opções
AZURE_STORAGE_CONTAINER=pncp
AZURE_BLOB_PREFIX=obras/sp
```

Se o `.env` não existir ou estiver vazio, o script roda normalmente e só
salva localmente — o upload é pulado com um aviso no console.

### Onde encontrar as credenciais do Azure

No [portal.azure.com](https://portal.azure.com):

1. **Storage accounts** → selecione a conta de destino.
2. **Security + networking → Access keys**: lá estão a **Connection string**
   e as **keys** (`key1`/`key2`).
3. O **nome da conta** aparece no topo da página (aba Overview).
4. O **container**: **Data storage → Containers** — se ainda não existir, não
   tem problema, o script cria automaticamente na primeira execução.

Se você não tem acesso ao portal, peça para quem administra a assinatura
Azure rodar:

```bash
az storage account show-connection-string --name NOME_DA_CONTA --resource-group NOME_DO_RESOURCE_GROUP
```

> As Access Keys dão acesso total à conta de armazenamento. Se precisar de
> um acesso mais restrito (só a um container específico), use um SAS token
> no lugar — isso exigiria um pequeno ajuste no script.

## Como usar

### Primeira execução (do zero)

```bash
python pncp_extract.py --anos 2022 2023 2024 --modo overwrite
```

Isso apaga qualquer bronze/checkpoint local existente e começa a coleta do
zero para os anos informados.

### Execuções seguintes / retomar após interrupção

```bash
python pncp_extract.py --anos 2022 2023 2024 --modo append
```

Em modo `append`, o script consulta o **checkpoint** (`data/checkpoint_pncp.json`)
e pula automaticamente as janelas (modalidade + período) que já foram
concluídas com sucesso em execuções anteriores — então interromper o
processo (Ctrl+C, queda de conexão, rate limit persistente) não faz perder
o progresso. É esse o modo recomendado no dia a dia.

### Só reprocessar o silver, sem consultar a API de novo

Útil se você mudou a lógica de classificação de `tipo_obra` e quer só
regerar o CSV a partir do bronze que já existe:

```bash
python pncp_extract.py --skip-ingest
```

### Rodar sem subir para o Azure

```bash
python pncp_extract.py --anos 2024 --skip-upload
```

Roda tudo normalmente, mas nunca toca no Azure — nem se o `.env` estiver
configurado. Bom para testar localmente.

### Todos os parâmetros

| Flag | Padrão | Descrição |
|---|---|---|
| `--anos` | `2022 2023 2024` | Anos a coletar. Ex: `--anos 2023 2024` |
| `--modo` | `append` | `append` mantém bronze/checkpoint existentes; `overwrite` apaga tudo e recomeça |
| `--skip-ingest` | desligado | Pula a consulta à API e só regenera o silver a partir do bronze existente |
| `--delay` | `1.5` | Segundos de espera entre chamadas à API do PNCP. Aumente (ex: `--delay 3`) se estiver tomando erro 429 (limite de requisições) com frequência |
| `--skip-upload` | desligado | Não sobe nada para o Azure, mesmo com `.env` configurado |

## Arquivos gerados

```
data/
├── bronze_pncp_contratacoes.jsonl    # payload bruto da API, 1 JSON por linha
├── checkpoint_pncp.json              # controle de janelas já concluídas (retomada)
└── silver_pncp_contratacoes_obras.csv # tabela final tratada
```

Se o Azure estiver configurado, os mesmos `bronze_pncp_contratacoes.jsonl` e
`silver_pncp_contratacoes_obras.csv` também aparecem no container definido
em `AZURE_STORAGE_CONTAINER`, dentro do prefixo `AZURE_BLOB_PREFIX` (se
informado).

## Quando o upload para o Azure acontece

O upload **não** espera o processo inteiro terminar:

```
para cada modalidade (Concorrência Eletrônica, Presencial, Pregão, Dispensa):
    processa todas as janelas dessa modalidade
      └─ a cada janela: grava bronze local + checkpoint local
    [modalidade concluída]
      └─ sobe o bronze acumulado até aqui para o Azure

[todas as modalidades concluídas]
gera o silver local
    └─ sobe o silver para o Azure
```

Ou seja, se o script for interrompido no meio de uma modalidade longa (o
Pregão Eletrônico, com ~159 janelas, é a mais demorada), o Azure já tem tudo
que foi concluído nas modalidades anteriores — só a modalidade em andamento
no momento da interrupção é que ainda não subiu.

## Entendendo o log durante a execução

```
  → Pregão Eletrônico | 159 janelas a processar | 1 worker(s)
    ✓ 202305 — 26 obras (de 30 contratos avaliados)
```

- **obras**: quantos contratos bateram com as palavras-chave de obra
- **contratos avaliados**: quantos contratos a API retornou no total para
  aquela janela, antes do filtro — se esse número for 0 sem nenhum erro
  reportado, é sinal de que a API genuinamente não tinha nada para aquele
  período/modalidade (não é bug)

Eventos de erro aparecem assim:

```
    ⏳ 202307 mod=4 pág=1 — 429, aguardando 30s (tentativa 2/5)
    ⚠️  202307 mod=4 — HTTP 403 (pág=1): '...'
    ❌ 202307 mod=4 pág=1 — desistindo (conexão falhou: ConnectionError)
```

- `⏳` = rate limit (429), o script está esperando e vai tentar de novo
  automaticamente (até 5 tentativas)
- `⚠️` = status HTTP inesperado ou JSON inválido — a janela é abandonada e
  registrada como evento, mas **não** entra no checkpoint, então será
  retentada na próxima execução em modo `append`
- `❌` = falha de conexão após 3 tentativas, mesmo comportamento acima

No final da ingestão aparece um bloco `=== Diagnóstico por modalidade ===`
resumindo quantas janelas tiveram evento e quantos contratos foram
avaliados por modalidade — útil para saber se "zero obras encontradas" é
dado real ou sintoma de algo falhando.

## Problemas comuns

**Erro 429 (limite de requisições) com frequência**
Aumente o `--delay` (ex: `--delay 3` ou mais) e rode de novo em modo
`append` — o checkpoint evita retrabalho do que já deu certo.


**Quero rodar em background com log salvo em arquivo**
Use `python -u` para garantir saída sem buffer:

```bash
python -u pncp_extract.py --anos 2022 2023 2024 > log.txt 2>&1 &
```