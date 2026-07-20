# tce.py — Ingestão Bronze/Silver TCE-SP AUDESP

Pipeline em **Python puro** (sem PySpark/Databricks) que baixa dados de licitações/contratos de obras do TCE-SP (AUDESP), grava a camada **bronze** particionada por período em Azure Blob Storage, e consolida a camada **silver** num único arquivo Parquet. Feito para rodar em VM Linux via `cron`, com checkpoint local para retomar execuções interrompidas.

## Visão geral do fluxo

```
TCE-SP (zip mensal)
      │
      ▼
 baixar_mes_tce()  ──►  filtra "Obras e servicos de engenharia"
      │
      ▼
ingest_tce_bronze()  ──►  Parquet particionado por período (Hive-style)
      │                   <container>/tce/bronze/periodo=YYYY-MM/part-<uuid>.parquet
      │
      ▼
  ingest_silver()  ──►  lê partições pendentes, limpa/tipa, empilha tudo
      │                 e sobrescreve um único arquivo consolidado
      ▼
<container>/tce/silver/tce_licitacoes_obras_consolidado.parquet
```

Bronze e silver têm **checkpoints independentes** em JSON local, então é possível rodar só uma etapa sem reprocessar a outra.

## Uso (linha de comando)

```bash
python tce.py              # roda bronze + silver (padrão)
python tce.py --bronze     # só a bronze (ingestão a partir da fonte TCE)
python tce.py --silver     # só a silver (transforma o que já está na bronze)
python tce.py --help       # ajuda
```

## Configuração (`.env`)

O `.env` fica na **raiz do projeto**, um nível acima da pasta deste script (`BASE_DIR/../.env`), no mesmo padrão usado no `pncp.py` — evita duplicar credenciais entre pipelines. Não versionar.

| Variável | Obrigatória? | Padrão | Descrição |
|---|---|---|---|
| `AZURE_STORAGE_CONNECTION_STRING` | Uma das duas opções de credencial | — | Connection string completa (opção mais simples) |
| `AZURE_STORAGE_ACCOUNT_NAME` + `AZURE_STORAGE_ACCOUNT_KEY` | Alternativa à connection string | — | Autenticação por conta + chave |
| `AZURE_STORAGE_CONTAINER` | Não | `conteiner` | Container único onde bronze e silver são gravados |
| `AZURE_BLOB_PREFIX_tce` | Não | `tce` | Prefixo do caminho — vira `tce/bronze/...` e `tce/silver/...` |
| `ANOS` | Não | `2022,2023,2024` | Anos a processar na bronze, separados por vírgula |
| `LOTE_MESES` | Não | `6` | Quantos meses acumular em memória antes de gravar um lote na bronze |
| `MODO_ESCRITA` | Não | `append` | `append` (soma) ou `overwrite` (apaga a partição antes de gravar) |
| `FONTE` | Não | `tce_audesp` | Mantido por compatibilidade — `tce_audesp` ou `ambas` |
| `CHECKPOINT_APPEND` | Não | `true` | `true` retoma do checkpoint; `false` ignora e reprocessa tudo |
| `CHECKPOINT_BRONZE_PATH` | Não | `checkpoint_bronze.json` (ao lado do script) | Caminho do checkpoint da bronze |
| `CHECKPOINT_SILVER_PATH` | Não | `checkpoint_silver.json` (ao lado do script) | Caminho do checkpoint da silver |

## Dependências

```bash
pip install pandas pyarrow requests python-dotenv azure-storage-blob zipfile-deflate64
```

## Camada Bronze

- Baixa o zip mensal (`licitacao-YYYY-MM_0.zip`) da TCE, lê o CSV **direto da memória** (sem tocar disco), filtrando `Objeto == "Obras e servicos de engenharia"`.
  - **Fallback deflate64**: se o zip usa compressão não suportada pelo `zipfile` nativo, salva um `.zip` temporário só para leitura via `pandas` e apaga em seguida.
- Acumula os DataFrames em memória (`lote`) até atingir `LOTE_MESES`, depois grava por período em Parquet no Azure (`upload_parquet`).
- **`MODO_ESCRITA=overwrite`** apaga os blobs existentes da partição (`limpar_particao`) antes de regravar — evita duplicação em reprocessamento.
- **Checkpoint por período** (não só por lote): assim que um período sobe com sucesso, ele já entra no `checkpoint_bronze.json`. Se o processo cair no meio, nada do que já foi salvo se perde.
- Períodos que falharem (download ou upload) são listados no log ao final e **não** entram no checkpoint — uma nova execução em modo append tenta de novo só esses.

## Camada Silver

- Lista os períodos já existentes na bronze e compara com o checkpoint da silver — só processa o que ainda não foi incorporado ao consolidado.
- Para cada período pendente: lê a partição bronze inteira, aplica `transformar_silver()` e guarda em memória.
- Empilha (`pd.concat`) todos os períodos novos, junta com o consolidado existente (se houver, baixando-o do Azure) e roda `drop_duplicates()` antes de sobrescrever o arquivo único.
- **`transformar_silver()`** aplica limpeza básica — ajustar conforme as regras de negócio reais:
  - Colunas de valor (`vl_unit_orcamento_lote`, `vl_unit_orcamento_item`, `vl_proposta`): normaliza separador de milhar/decimal (formato BR) e converte para numérico.
  - Colunas de quantidade (`qtd_contratada`, `qtd_orcamento_lote`, `qtd_orcamento_item`): converte para numérico.
  - `dt_edital` e `dt_ingestao`: parseadas como data (`dt_edital` assume `dayfirst=True`).
  - Colunas de texto: `strip()` nas pontas.
  - Deduplicação de linhas.
- **Importante**: como o consolidado é reescrito por completo a cada execução (baixa tudo → junta → sobe tudo), o custo cresce com o tamanho total da base, não só com o que é novo. Para o volume atual isso é tranquilo; se a base crescer muito (muitos milhões de linhas), vale revisar para um formato mais incremental.

## Checkpoint local

Dois arquivos JSON independentes (bronze e silver), gravados de forma atômica (escreve em `.tmp` e renomeia) para nunca corromper em caso de interrupção:

```json
{
  "periodos_concluidos": ["2022-01", "2022-02", "..."],
  "atualizado_em": "2026-07-20T14:16:37"
}
```

- `CHECKPOINT_APPEND=true` (padrão): pula períodos já no checkpoint.
- `CHECKPOINT_APPEND=false`: ignora o checkpoint existente e reprocessa tudo do zero (não apaga o arquivo, só não o consulta nessa execução — ao final, o checkpoint é sobrescrito com o resultado da nova rodada).

## Tratamento de erros

- **`Ctrl+C`**: mensagem informando que o progresso no checkpoint não foi perdido, encerra sem stack trace.
- **Erro não previsto**: imprime o traceback completo e **relança a exceção** (`raise`), garantindo exit code ≠ 0 — importante para agendadores (cron, Task Scheduler) detectarem falha.

## Estrutura de caminhos no Azure

```
<container>/
└── tce/
    ├── bronze/
    │   ├── periodo=2022-01/
    │   │   └── part-<uuid>.parquet
    │   ├── periodo=2022-02/
    │   │   └── part-<uuid>.parquet
    │   └── ...
    └── silver/
        └── tce_licitacoes_obras_consolidado.parquet
```

## Logging

- Todos os logs vão para `stdout` no formato `timestamp | LEVEL | mensagem`.
- Os loggers internos do SDK do Azure (`azure`, `azure.core.pipeline.policies.http_logging_policy`) são elevados para `WARNING`, evitando poluição com detalhes de request/response HTTP.
- Na inicialização, o script loga onde encontrou o `.env`, o container e caminhos efetivos de bronze/silver — útil para depurar problemas de variável de ambiente não carregada.