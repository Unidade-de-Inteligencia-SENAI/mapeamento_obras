# tce.py — Ingestão Bronze/Silver TCE-SP AUDESP

Pipeline em **Python puro**  que baixa dados de licitações/contratos de obras do TCE-SP (AUDESP) e salva tudo no **Azure Blob Storage**. Feito para rodar em VM Linux via `cron`, com checkpoint local para retomar execuções interrompidas.

## Visão geral do fluxo

```
TCE-SP (zip mensal)
      │
      ▼
 baixar_mes_tce()  ──►  lê o CSV em pedaços (chunks), filtra
      │                 "Obras e servicos de engenharia" na hora
      ▼
ingest_tce_bronze()  ──►  baixa o bronze existente do Azure (memória),
      │                   mescla com os períodos novos, sobe por lote
      ▼
<container>/<prefixo>/bronze/tce_licitacoes_obras.parquet
      │
      ▼
  ingest_silver()  ──►  baixa o bronze inteiro, aplica transformar_silver(),
      │                 reprocessa tudo do zero (não é incremental)
      ▼
<container>/<prefixo>/silver/tce_licitacoes_obras_consolidado.parquet
```

**Só a bronze tem checkpoint.** A silver não tem estado próprio — ela sempre reprocessa o bronze inteiro a cada execução, então não existe risco de ficar "desatualizada" ou precisar de lógica incremental separada.

**Garantia importante:** um período só entra no checkpoint **depois** que o upload do lote correspondente é confirmado no Azure — nunca antes. Se o upload falhar ou o processo for interrompido no meio de um lote, esses períodos simplesmente não entram no checkpoint e são refeitos automaticamente na próxima execução — nunca ficam num estado "marcado como feito, mas sem dado".

## Uso (linha de comando)

```bash
python tce.py                                    # bronze + silver, anos padrão (2022-2024), modo append
python tce.py --anos 2022 2023 2024 --modo append
python tce.py --anos 2024 --modo overwrite        # ignora bronze existente no Azure, recomeça do zero
python tce.py --skip-ingest                       # só regenera a silver a partir do bronze já existente
python tce.py --anos 2024 --skip-upload           # teste pontual — checkpoint não avança, ver aviso
python tce.py --lote-meses 3                      # sobe pro Azure a cada 3 meses em vez de 6 (padrão)
python tce.py --help
```

### Todos os parâmetros

| Flag | Padrão | Descrição |
|---|---|---|
| `--anos` | `2022 2023 2024` | Anos a coletar. Ex: `--anos 2023 2024` |
| `--modo` | `append` | `append` mescla com o bronze/silver já existentes no Azure; `overwrite` ignora tudo e recomeça do zero (limpa o checkpoint também) |
| `--skip-ingest` | desligado | Pula a etapa bronze e só regenera a silver a partir do bronze existente no Azure — zero downloads do TCE |
| `--lote-meses` | `6` | Quantos meses acumular em memória antes de subir um lote para o Azure |
| `--skip-upload` | desligado | Não sobe nada para o Azure. O checkpoint não avança nesta execução — use só para testes pontuais |

## Configuração (`.env`)

O `.env` fica na **raiz do projeto**, um nível acima da pasta deste script (`BASE_DIR/../.env`), mesmo padrão do `pncp.py` — evita duplicar credenciais entre pipelines. Não versionar.

| Variável | Obrigatória? | Padrão | Descrição |
|---|---|---|---|
| `AZURE_STORAGE_CONNECTION_STRING` | Uma das duas opções de credencial | — | Connection string completa (opção mais simples) |
| `AZURE_STORAGE_ACCOUNT_NAME` + `AZURE_STORAGE_ACCOUNT_KEY` | Alternativa à connection string | — | Autenticação por conta + chave |
| `AZURE_STORAGE_CONTAINER` | Não | `conteiner` | Container onde bronze e silver são gravados |
| `AZURE_BLOB_PREFIX_tce` | Não | `tce` | Prefixo do caminho — vira `tce/bronze/...` e `tce/silver/...` |
| `CHECKPOINT_PATH` | Não | `data/checkpoint_tce.json` (um nível acima da pasta do script) | Caminho do checkpoint |

`ANOS`, `MODO_ESCRITA`, `LOTE_MESES` e `FONTE` **saíram do `.env`** e viraram argumentos de linha de comando (`--anos`, `--modo`, `--lote-meses`). `CHECKPOINT_APPEND`, `CHECKPOINT_BRONZE_PATH` e `CHECKPOINT_SILVER_PATH` deixaram de existir (só há um checkpoint agora, controlado por `--modo`).

## Dependências

```bash
pip install pandas pyarrow requests python-dotenv azure-storage-blob zipfile-deflate64
```

## Camada Bronze

- Baixa o zip mensal (`licitacao-YYYY-MM_0.zip`) do TCE. Se a conexão falhar (timeout, erro de rede), tenta de novo até 3 vezes com espera crescente (10s, 20s) antes de desistir daquele período.
- **Lê o CSV em pedaços de 50 mil linhas** (`chunksize`), filtrando `Objeto == "Obras e servicos de engenharia"** a cada pedaço e descartando o resto na hora — em vez de carregar o mês inteiro (todas as licitações de SP, não só obras, potencialmente centenas de milhares de linhas) inteiro na memória antes de filtrar. Essa é a correção para o `Killed` por falta de memória (OOM) que aconteceu numa VM com 7.6GB de RAM processando poucos meses.
  - **Fallback deflate64**: se o zip usa compressão não suportada pelo `zipfile` nativo, salva um `.zip` temporário só para leitura (também em chunks) via `pandas`, e apaga em seguida.
  - Cada linha de log de período mostra a memória residente do processo (`memória do processo: NNN MB`) — visibilidade extra pra flagrar crescimento anormal sem precisar caçar no `dmesg`.
- No início da execução (modo `append`), baixa o bronze existente do Azure para a memória (nunca grava em disco).
- Acumula os meses processados em memória até atingir `--lote-meses`, então mescla com o bronze já existente e sobe o combinado para o Azure — repete a cada lote concluído, não só no final.
- **Checkpoint só é atualizado depois que o upload do lote é confirmado**: os períodos daquele lote só entram no `checkpoint_tce.json` se o `upload_blob` retornar sucesso. Se falhar, nada é marcado — esses períodos são refeitos automaticamente na próxima execução em `--modo append`.
- Períodos sem dado ou com falha de download ficam listados no log ao final (`falhos`) e nunca entram no checkpoint.

## Camada Silver

- **Sempre reprocessa o bronze inteiro**, do zero, a cada execução — não lê "só o que é novo". Isso elimina a necessidade de um checkpoint próprio para a silver e qualquer risco de divergência entre bronze e silver.
- Se `--skip-ingest` foi usado (ou a etapa bronze não rodou nesta execução por outro motivo), baixa o bronze completo do Azure para a memória antes de transformar.
- **`transformar_silver()`** aplica a limpeza (inalterada em relação à versão anterior):
  - Colunas de valor (`vl_unit_orcamento_lote`, `vl_unit_orcamento_item`, `vl_proposta`): normaliza separador de milhar/decimal (formato BR) e converte para numérico.
  - Colunas de quantidade (`qtd_contratada`, `qtd_orcamento_lote`, `qtd_orcamento_item`): converte para numérico.
  - `dt_edital` e `dt_ingestao`: parseadas como data (`dt_edital` assume `dayfirst=True`).
  - Colunas de texto: `strip()` nas pontas.
  - Deduplicação de linhas.
- Sobe o resultado, sobrescrevendo o parquet consolidado no Azure (a menos que `--skip-upload` esteja ativo).
- **Ponto de atenção**: como agora tanto bronze quanto silver são arquivos únicos reescritos por completo a cada execução, o custo de cada upload cresce com o tamanho total da base acumulada, não só com o que é novo. Para o volume atual (dezenas de milhares de linhas por ano) isso é tranquilo; se a base ficar muito grande (muitos milhões de linhas), vale revisar para um formato mais incremental/particionado.

## Checkpoint local

Um único arquivo (`data/checkpoint_tce.json`, um nível acima da pasta do script), gravado de forma atômica (escreve em `.tmp` e renomeia) para nunca corromper em caso de interrupção — inclusive um `Killed` por falta de memória no meio da escrita:

```json
{
  "periodos_concluidos": ["2022-01", "2022-02", "..."],
  "atualizado_em": "2026-08-19T10:35:12"
}
```

- `--modo append` (padrão): pula períodos já no checkpoint.
- `--modo overwrite`: apaga o checkpoint existente e reprocessa tudo do zero.

**Importante:** o checkpoint só é criado/atualizado quando um **lote inteiro** (`--lote-meses`, padrão 6) termina com upload confirmado. Se a execução for interrompida antes de completar o primeiro lote, é esperado que o arquivo de checkpoint ainda nem exista — isso não é um bug, é a garantia funcionando (nada foi confirmado no Azure, então nada deveria estar marcado como concluído).

## Tratamento de erros

- **`Ctrl+C`**: mensagem explicando que o checkpoint só marca um período como concluído depois do upload confirmado — o que não deu tempo de subir simplesmente será refeito na próxima execução, sem stack trace no log.
- **Erro não previsto**: imprime o traceback completo e **relança a exceção** (`raise`), garantindo exit code ≠ 0 — importante para agendadores (cron, Task Scheduler) detectarem falha.
- **`Killed` sem traceback nenhum**: não é um erro do Python — é o OOM killer do Linux matando o processo à força (`SIGKILL`, que nenhum código consegue capturar). Confirme com:
  ```bash
  free -h
  dmesg | tail -20 | grep -i "killed process\|out of memory"
  ```
  A leitura em chunks já reduz bastante o risco disso acontecer; se persistir, considere reduzir `--lote-meses` ou aumentar a memória/swap da VM.

## Estrutura de caminhos no Azure

```
<container>/
└── <prefixo (AZURE_BLOB_PREFIX, padrão "tce")>/
    ├── bronze/
    │   └── tce_licitacoes_obras.parquet
    └── silver/
        └── tce_licitacoes_obras_consolidado.parquet
```


## Logging

- Todos os logs vão para `stdout` no formato `timestamp | LEVEL | mensagem`.
- Os loggers internos do SDK do Azure (`azure`, `azure.core.pipeline.policies.http_logging_policy`) são elevados para `WARNING`, evitando poluição com detalhes de request/response HTTP.
- Na inicialização, o script loga onde encontrou o `.env`, o container e o prefixo efetivo.
- Cada período processado loga também a memória residente do processo (`ru_maxrss`), pra dar visibilidade de crescimento de memória sem precisar do `dmesg`.