# pncp.py

Script Python puro que extrai contratações de **obras**
do Portal Nacional de Contratações Públicas (PNCP) para o estado de SP,
transforma em uma tabela tratada e salva tudo no **Azure Blob Storage**. Não existe cópia local dos dados — só um arquivo de checkpoint pequeno, usado para retomar execuções interrompidas.

## O que ele faz

1. **Bronze** — consulta a API pública do PNCP (`/v1/contratacoes/publicacao`)
   para 4 modalidades de contratação (Concorrência Eletrônica, Concorrência
   Presencial, Pregão Eletrônico, Dispensa de Licitação), varrendo janelas de
   data (mensais ou semanais, dependendo do volume da modalidade) para os
   anos pedidos. Filtra apenas contratos cujo objeto contenha palavras
   relacionadas a obras (`obra`, `construção`, `paviment`, `reforma`,
   `saneamento`, `drenagem`, `ponte`, `habitação` etc.) e mantém o payload
   bruto **em memória**, em formato JSONL (1 JSON por linha).

2. **Silver** — a partir do bronze (baixado do Azure ou já em memória),
   achata os campos aninhados (órgão, unidade, amparo legal), classifica
   cada contrato num `tipo_obra` (pavimentação, saneamento, viário,
   habitação, drenagem, equipamento social/esportivo, construção geral) via
   regex no objeto da compra, remove duplicados e serializa como **Parquet**.

3. **Upload para o Azure** — bronze e silver são enviados direto de um
   buffer em memória (nunca passam pelo disco). O bronze sobe ao final de
   **cada modalidade** concluída (não só no fim do processo inteiro); o
   silver sobe uma vez, ao final da transformação.

**Garantia importante:** uma janela só entra no checkpoint **depois** que o
upload do lote correspondente é confirmado no Azure — nunca antes. Isso
significa que "está no checkpoint" sempre implica "o dado está salvo no
Azure", mesmo que o processo seja interrompido no meio de uma modalidade.
Se o upload falhar ou o script for interrompido antes dele, aquelas janelas
simplesmente não entram no checkpoint e são refeitas automaticamente na
próxima execução — nunca ficam num estado "marcado como feito, mas sem
dado".

## Pré-requisitos

- Python 3.9+
- Dependências (arquivo `requirements.txt`):

```bash
pip install -r requirements.txt
```

Isso instala `requests`, `pandas`, `pyarrow` (necessário para ler/escrever
o silver em Parquet), `python-dotenv` e `azure-storage-blob`.

## Configuração do Azure

1. Copie `.env.example` para `.env`, na **raiz do projeto** (um nível acima
   da pasta onde `pncp.py` está — o script já sabe procurar lá,
   independente de onde você rodar o comando):

```bash
cp .env.example .env
```

2. Preencha o `.env`. Só é preciso preencher **uma** das duas opções de
   credencial:

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

3. **Nunca commite o `.env`.** Garanta que ele está no `.gitignore` do
   repositório antes do primeiro commit.

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

Para conferir que os arquivos realmente estão lá (mais confiável que
navegar pelo portal, que às vezes fica com cache desatualizado):

```bash
az storage blob list --account-name SUA_CONTA --container-name SEU_CONTAINER --prefix bronze/ --output table
az storage blob list --account-name SUA_CONTA --container-name SEU_CONTAINER --prefix silver/ --output table
```

> As Access Keys dão acesso total à conta de armazenamento. Se precisar de
> um acesso mais restrito (só a um container específico), use um SAS token
> no lugar — isso exigiria um pequeno ajuste no script.

## Como usar

### Primeira execução (do zero)

```bash
python pncp.py --anos 2022 2023 2024 --modo overwrite
```

Ignora qualquer bronze existente no Azure e limpa o checkpoint local,
começando a coleta do zero para os anos informados.

### Execuções seguintes / retomar após interrupção

```bash
python pncp.py --anos 2022 2023 2024 --modo append
```

Em modo `append`, o script:
1. Baixa o bronze existente do Azure para a memória (sem gravar em disco).
2. Consulta o checkpoint local e pula automaticamente as janelas
   (modalidade + período) já concluídas.
3. Mescla o que coletar de novo com o que já existia, em memória.
4. Sobe o resultado combinado ao final de cada modalidade.

Interromper o processo (Ctrl+C, queda de conexão, rate limit persistente)
não corrompe nada — o que já tinha sido confirmado no Azure continua lá, e
o que não deu tempo de subir simplesmente é refeito na próxima execução.
Este é o modo recomendado no dia a dia.

**Nota sobre publicação retroativa:** o checkpoint marca uma janela como
concluída na primeira consulta sem erro. Se o PNCP receber uma publicação
tardia com data de referência dentro de um período já checkpointado, esse
dado novo não vai ser recapturado automaticamente — não existe hoje uma
janela de segurança/reprocessamento automático dos últimos meses.

### Só reprocessar o silver, sem consultar a API de novo

Útil se você mudou a lógica de classificação de `tipo_obra` (ou qualquer
outra parte da transformação) e quer só regerar o Parquet a partir do
bronze que já existe no Azure — zero chamadas à API do PNCP:

```bash
python pncp.py --skip-ingest
```

### Todos os parâmetros

| Flag | Padrão | Descrição |
|---|---|---|
| `--anos` | `2022 2023 2024` | Anos a coletar. Ex: `--anos 2023 2024` |
| `--modo` | `append` | `append` mescla com bronze/silver já existentes no Azure; `overwrite` ignora tudo e recomeça do zero (limpa o checkpoint também) |
| `--skip-ingest` | desligado | Pula a consulta à API e só regenera o silver a partir do bronze existente no Azure |
| `--delay` | `1.5` | Segundos de espera entre chamadas à API do PNCP. Aumente (ex: `--delay 3`) se estiver tomando erro 429 (limite de requisições) com frequência |
| `--skip-upload` | desligado | Não sobe nada para o Azure. O checkpoint não avança nesta execução — use só para testes |

## Onde as coisas ficam

```
data/
└── checkpoint_pncp.json    # único arquivo local — controle de retomada, sem dado de negócio
```

No Azure, dentro do container definido em `AZURE_STORAGE_CONTAINER`,
usando `AZURE_BLOB_PREFIX` como prefixo:

```
{container}/{AZURE_BLOB_PREFIX}/bronze/bronze_pncp_contratacoes.jsonl
{container}/{AZURE_BLOB_PREFIX}/silver/silver_pncp_contratacoes_obras.parquet
```

Ex: com `AZURE_STORAGE_CONTAINER=meucontainer` e `AZURE_BLOB_PREFIX=pncp`,
o bronze fica em `meucontainer/pncp/bronze/bronze_pncp_contratacoes.jsonl`
e o silver em `meucontainer/pncp/silver/silver_pncp_contratacoes_obras.parquet`.

## Quando o upload para o Azure acontece

```
carrega checkpoint local
baixa bronze existente do Azure (memória, sem disco) — se modo=append
para cada modalidade (Concorrência Eletrônica, Presencial, Pregão, Dispensa):
    para cada janela ainda não concluída:
        ✓ sucesso → fica em memória (checkpoint local só é gravado depois do upload)
    [modalidade termina]
        → se coletou algo novo nesta modalidade: sobe o bronze combinado pro Azure
          → só em caso de sucesso, o checkpoint é atualizado no disco
        → se não coletou nada novo (tudo já estava no checkpoint): upload é pulado

[as 4 modalidades terminaram]
    → lê o bronze inteiro (memória), reprocessa tudo, sobe o silver pro Azure
```

Pontos importantes:
- O upload do bronze **não** espera o processo inteiro terminar — acontece
  a cada modalidade concluída, então interromper no meio do Pregão
  Eletrônico (a modalidade mais longa, ~159 janelas) não faz perder o que
  já tinha sido processado nas modalidades anteriores.
- Se uma modalidade não tinha nenhuma janela nova para processar (tudo já
  no checkpoint), o script **não** reenvia o bronze — evita upload
  redundante quando não há nada de novo.
- O silver **sempre** reprocessa o bronze inteiro do zero a cada execução
  (não é incremental) e só é gerado/enviado depois que a ingestão inteira
  terminar — não existe upload parcial do silver.

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

Quando uma modalidade não tem nada novo a processar (tudo já no
checkpoint), aparece:

```
  → Concorrência Eletrônica | 0 janelas a processar (12 já concluídas via checkpoint) | 1 worker(s)
    ↷ Nada novo em 'Concorrência Eletrônica' — upload pulado (bronze no Azure já está atualizado)
```

Eventos de erro durante uma janela aparecem assim:

```
    ⏳ 202307 mod=4 pág=1 — 429, aguardando 30s (tentativa 2/5)
    ⏳ 202201 mod=4 pág=1 — ConnectionError, aguardando 10s (tentativa 2/4)
    ⚠️  202307 mod=4 — HTTP 403 (pág=1): '...'
    ❌ 202201 mod=4 pág=1 — desistindo (conexão falhou (pág=1) após 4 tentativas: ConnectionError)
```

- `⏳ ... 429` = rate limit — o script espera (respeitando o header
  `Retry-After` quando o PNCP manda) e tenta de novo automaticamente, até
  5 tentativas
- `⏳ ... ConnectionError/Timeout` = falha de conexão intermitente — o
  script tenta de novo com espera crescente (5s, 10s, 15s, 20s) e uma
  sessão HTTP nova a cada tentativa, até 4 tentativas
- `⚠️` = status HTTP inesperado ou JSON inválido — a janela é abandonada e
  registrada como evento; como não houve sucesso, ela **não** entra no
  checkpoint e será retentada na próxima execução em modo `append`
- `❌` = desistência definitiva daquela janela após esgotar as tentativas
  (429 ou conexão) — mesmo comportamento acima, não entra no checkpoint

No final da ingestão aparece um bloco `=== Diagnóstico por modalidade ===`
resumindo quantas janelas tiveram evento e quantos contratos foram
avaliados por modalidade — útil para saber se "zero obras encontradas" é
dado real ou sintoma de algo falhando.

## Problemas comuns

**Erro 429 (limite de requisições) com frequência**
Aumente o `--delay` (ex: `--delay 3` ou mais) e rode de novo em modo
`append` — o checkpoint evita retrabalho do que já deu certo.

**Erro de conexão intermitente (`ConnectionError`)**
Já tem retry automático embutido (até 4 tentativas, com espera crescente).
Se mesmo assim continuar desistindo com frequência, pode ser
instabilidade de rede da própria máquina/VM — vale testar
`curl https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao` direto
para isolar se o problema é da rede local ou da API.

**Upload parece ter dado certo no log, mas não aparece no Azure**
O log só imprime `✓ Upload OK` depois que o SDK confirma o envio — não é
um log otimista. Antes de desconfiar do script, confira:
1. Está olhando dentro da pasta virtual certa (`bronze/` ou `silver/`
   dentro do container, não na raiz)?
2. A `AZURE_STORAGE_CONNECTION_STRING`/`AZURE_STORAGE_ACCOUNT_NAME` no
   `.env` aponta pra mesma storage account que você está navegando no
   portal?
3. Existe alguma política de *lifecycle management* na conta que expira
   blobs automaticamente (comum em contas de "sandbox")?
4. Confirme via `az storage blob list` (comando na seção de configuração
   acima) em vez de confiar só no portal, que às vezes fica com cache
   desatualizado.
5. Confira se `AZURE_BLOB_PREFIX` está escrito com o mesmo nome exato no
   `.env` e no script — um typo faz o prefixo virar vazio silenciosamente
   (ver aviso na seção de configuração).

**Quero rodar em background com log salvo em arquivo**
O script já força saída sem buffer (`print` com `flush=True`), mas por
segurança use `python -u` também:

```bash
python -u pncp.py --anos 2022 2023 2024 > log.txt 2>&1 &
```