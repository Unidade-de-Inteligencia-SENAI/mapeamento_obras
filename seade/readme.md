# seade.py — Ingestão Bronze/Silver SEADE (Investimentos)

Pipeline em **Python puro** que baixa as bases de investimentos do SEADE (SP) e salva tudo no **Azure Blob Storage**. 

Diferente do PNCP e do TCE (que varrem janelas de data ao longo do tempo), o SEADE é um número fixo e pequeno de **fontes cadastradas** — hoje 3 datasets de investimentos captados. Cada execução simplesmente atualiza essas fontes com o dado mais recente disponível.

## Visão geral do fluxo

```
SEADE (CSV/XLSX/ZIP/7z, por fonte)
      │
      ▼
_baixar_para_memoria()  ──►  download com barra de progresso, retry em falha
      │                      de conexão (até 3x, espera crescente)
      ▼
_extrair_tabulares()  ──►  se vier em zip/7z, extrai o(s) CSV/XLSX de dentro
      │                    (scratch temporário só para .7z, apagado na hora)
      ▼
processar_fonte_bronze()  ──►  sobe o(s) arquivo(s) tabular(es) CRUS, como vieram
      │                        <container>/<prefixo>/bronze/<fonte>/<arquivo>
      ▼
processar_fonte_silver()  ──►  lê, normaliza nomes de coluna (sem acento/
      │                        espaço/maiúscula), sobe como Parquet único
      ▼
<container>/<prefixo>/silver/<fonte>.parquet
```

Uma fonte só entra no checkpoint depois que **bronze e silver** são confirmados no Azure.

## Uso (linha de comando)

```bash
python seade.py                                   # processa as 3 fontes cadastradas
python seade.py --fontes investimentos_captados    # só uma fonte específica
python seade.py --fontes investimentos_captados investimentos_captados_sem_valor  # duas
python seade.py --modo overwrite                   # ignora checkpoint, reprocessa tudo
python seade.py --skip-download                    # reprocessa a silver a partir do bronze já no Azure
python seade.py --skip-upload                      # teste pontual — checkpoint não avança
python seade.py --help
```

### Todos os parâmetros

| Flag | Padrão | Descrição |
|---|---|---|
| `--fontes` | todas as cadastradas em `FONTES` | Nomes das fontes a processar. Ex: `--fontes investimentos_captados` |
| `--modo` | `append` | `append` pula fontes já concluídas no checkpoint; `overwrite` apaga o checkpoint e reprocessa tudo |
| `--skip-download` | desligado | Não baixa da fonte original (SEADE) — baixa o bronze já existente no Azure e só regenera a silver. Útil pra testar mudanças em `_normalizar_coluna`/`_ler_tabular` sem gastar banda |
| `--skip-upload` | desligado | Não sobe nada para o Azure. O checkpoint não avança — use só para testes pontuais |

## Fontes cadastradas

Hoje há 3, definidas na lista `FONTES` no topo do script:

| Nome | Conteúdo |
|---|---|
| `investimentos_captados` | Investimentos captados (base geral) |
| `investimentos_captados_com_valor` | Investimentos confirmados, com valor |
| `investimentos_captados_sem_valor` | Investimentos confirmados, sem valor |

Cada entrada tem `nome`, `url` (link direto de download), `delimitador` (`;` para os CSVs atuais) e `encoding` (`windows-1252`). Para adicionar uma nova fonte, basta acrescentar um dicionário nessa lista — não precisa mexer em mais nada do resto do script.

## Configuração (`.env`)

O `.env` fica na **raiz do projeto**, um nível acima da pasta deste script (`BASE_DIR/../.env`), mesmo padrão do `pncp.py`/`tce.py` — evita duplicar credenciais entre pipelines. Não versionar.

| Variável | Obrigatória? | Padrão | Descrição |
|---|---|---|---|
| `AZURE_STORAGE_CONNECTION_STRING` | Uma das duas opções de credencial | — | Connection string completa (opção mais simples) |
| `AZURE_STORAGE_ACCOUNT_NAME` + `AZURE_STORAGE_ACCOUNT_KEY` | Alternativa à connection string | — | Autenticação por conta + chave |
| `AZURE_STORAGE_CONTAINER` | Não | `conteiner` | Container onde bronze e silver são gravados |
| `AZURE_BLOB_PREFIX_seade` | Não | `seade` | Prefixo do caminho — vira `seade/bronze/...` e `seade/silver/...` |
| `CHECKPOINT_PATH` | Não | `data/checkpoint_seade.json` (um nível acima da pasta do script) | Caminho do checkpoint |

## Dependências

```bash
pip install pandas pyarrow requests python-dotenv azure-storage-blob tqdm unidecode py7zr
```

`py7zr` só é usado se alguma fonte vier compactada em `.7z` — se todas as fontes forem CSV/XLSX/ZIP (como as 3 atuais), o script funciona mesmo sem esse pacote instalado (só avisa e falha aquela fonte específica se algum dia precisar dele).

## Camada Bronze

- Baixa a URL da fonte inteira para a memória, com barra de progresso (`tqdm`). Falha de conexão tenta de novo até 3 vezes, com espera crescente (10s, 20s).
- Se o arquivo baixado for `.zip` ou `.7z`, extrai o(s) CSV/XLSX de dentro (múltiplos arquivos são suportados — todos são lidos e empilhados na etapa silver).
  - **7z que na verdade é zip disfarçado**: se a extração via `py7zr` falhar, tenta de novo como `.zip` — já aconteceu na prática com alguns arquivos do governo.
- Sobe o(s) arquivo(s) tabular(es) **exatamente como vieram** (sem nenhuma transformação) para `bronze/<fonte>/<nome_do_arquivo>`.
- Retry de 3 tentativas com 15s de espera em caso de falha no upload.

## Camada Silver

- Lê o(s) arquivo(s) tabular(es) da fonte (recém-baixados, ou vindos do bronze existente se `--skip-download`).
- **Fallback de encoding**: se a leitura do CSV falhar com `UnicodeDecodeError` (comum quando o encoding declarado, ex: `windows-1252`, tem algum byte indefinido no meio do arquivo — sistemas legados do governo geram isso ocasionalmente), tenta de novo com `encoding_errors="replace"`, trocando só os bytes inválidos por `�` em vez de abortar a fonte inteira. Um aviso é logado quando isso acontece — vale conferir depois se o `�` caiu num campo sem importância.
- Normaliza nomes de coluna: remove acento (`unidecode`), tira espaço nas pontas, troca espaço por `_`, troca `$` por `s`, deixa tudo minúsculo, remove qualquer caractere que não seja `a-z0-9_`. Ex: `"Valor Investido ($)"` → `valor_investido_s`.
- Se a fonte tiver múltiplos arquivos tabulares (caso de zip com mais de um CSV), todos são concatenados (`pd.concat`) antes de normalizar e subir.
- Sobe o resultado como Parquet único, sobrescrevendo `silver/<fonte>.parquet`.

## Checkpoint local

Um único arquivo (`data/checkpoint_seade.json`), gravado de forma atômica (escreve em `.tmp` e renomeia):

```json
{
  "fontes_concluidas": ["investimentos_captados", "investimentos_captados_sem_valor"],
  "atualizado_em": "2026-08-19T13:27:04"
}
```

- `--modo append` (padrão): pula fontes já no checkpoint.
- `--modo overwrite`: apaga o checkpoint e reprocessa todas as fontes.
- Uma fonte só entra aqui depois que **bronze e silver** são confirmados no Azure — se uma fonte falhar (download, extração, leitura, ou upload), ela simplesmente não entra, e é retentada automaticamente na próxima execução em `--modo append`. As outras fontes que deram certo não são afetadas.

## Tratamento de erros

- **`Ctrl+C`**: mensagem explicando que o checkpoint só marca uma fonte como concluída depois que bronze e silver são confirmados — o que não deu tempo de subir será refeito na próxima execução, sem stack trace no log.
- **Erro não previsto**: imprime o traceback completo e **relança a exceção** (`raise`), garantindo exit code ≠ 0 — importante para agendadores (cron) detectarem falha.
- **Falha isolada por fonte** (download, extração, encoding, upload): não derruba as outras fontes da mesma execução — cada uma é processada de forma independente, e só a que falhou fica de fora do checkpoint.

## Estrutura de caminhos no Azure

```
<container>/
└── <prefixo (AZURE_BLOB_PREFIX, padrão "seade")>/
    ├── bronze/
    │   ├── investimentos_captados/
    │   │   └── piesp_captados.csv
    │   ├── investimentos_captados_com_valor/
    │   │   └── piesp_confirmados_com_valor.csv
    │   └── investimentos_captados_sem_valor/
    │       └── piesp_confirmados_sem_valor.csv
    └── silver/
        ├── investimentos_captados.parquet
        ├── investimentos_captados_com_valor.parquet
        └── investimentos_captados_sem_valor.parquet
```

## Logging

- Todos os logs vão para `stdout` no formato `timestamp | LEVEL | mensagem`.
- Os loggers internos do SDK do Azure são elevados para `WARNING`, evitando poluição com detalhes de request/response HTTP.
- Na inicialização, o script loga onde encontrou o `.env`, o container e o prefixo efetivo.
- Barra de progresso (`tqdm`) durante o download de cada fonte, mostrando tamanho baixado e velocidade.

## Problemas comuns

**`UnicodeDecodeError` ao ler o CSV**
Já tem fallback automático (`encoding_errors="replace"`) — a fonte não deveria mais falhar por causa disso. Se aparecer o aviso no log, vale checar o silver gerado pra confirmar que o `�` caiu num campo de texto e não num campo numérico/identificador importante.

**Fonte falha sempre no download**
O script já tenta 3 vezes com espera crescente. Se persistir, confira se a URL ainda é válida — o SEADE ocasionalmente muda o link de download de um recurso quando republica um dataset; nesse caso é preciso atualizar a `url` correspondente em `FONTES`.

**Quero adicionar uma nova fonte SEADE**
Só acrescentar um novo dicionário em `FONTES`, com `nome`, `url`, `delimitador` e `encoding`. Não precisa alterar mais nada — bronze, silver e checkpoint já cobrem qualquer fonte nova automaticamente.

**Quero rodar em background com log salvo em arquivo**
```bash
python -u seade.py > log.txt 2>&1 &
```