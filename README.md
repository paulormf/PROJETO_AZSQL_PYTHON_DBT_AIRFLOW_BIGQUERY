# Data Engineering Portfolio Project

Projeto de treinamento para extrair dados do Azure SQL e, nas próximas etapas,
carregá-los e transformá-los com BigQuery, dbt e Airflow.

## Conexão local com Azure SQL

### Pré-requisitos

- Python instalado.
- Microsoft ODBC Driver 18 for SQL Server instalado.
- Acesso de rede ao servidor e seu IP liberado no firewall do Azure SQL.
- Um login com as permissões mínimas necessárias, preferencialmente somente leitura.

### Configuração

No PowerShell, na raiz do repositório:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
Copy-Item .env.example .env
```

Preencha `.env` localmente. Esse arquivo é ignorado pelo Git; não coloque
credenciais em scripts, notebooks ou commits.

## Exportar Azure SQL para GCS como Parquet

O contrato de schema compartilhado por full e CDC fica em `src/sql_schema.py`.
Ele consulta os tipos da tabela no catálogo do Azure SQL, aplica um mapeamento
versionado para Arrow/Parquet e registra o contrato no manifesto. Os dois
carregadores informam schema explícito ao BigQuery e validam as tabelas de
staging; o CDC também compara o contrato com a tabela Bronze antes de aplicar
alterações. Tipos SQL sem mapeamento causam falha antes de publicar o manifesto
de sucesso. Ao alterar o schema da origem, faça uma migração/rebaseline
intencional antes de retomar o CDC.

O pipeline usa uma transação `SNAPSHOT` única para contar e extrair todas as tabelas habilitadas. Os manifestos por tabela e `_RUN_SUCCESS.json` só são publicados depois do commit. A transação longa pode reter versões de linhas e consumir recursos na origem; para bases muito grandes, prefira extrair de uma cópia transacional do banco ou use uma estratégia de bootstrap coordenada com CDC.

Neste projeto, `columns: "*"` no YAML leva todas as colunas da origem. O script compara `COUNT_BIG(*)` na origem com as linhas exportadas. Só publica `_SUCCESS.json` após todos os Parquet serem enviados e as contagens coincidirem; divergência gera `_FAILED.json`. O manifesto lista cada objeto, suas linhas e seu tamanho. Uma falha antes da publicação do marcador significa que a execução está incompleta.

O pipeline `pipelines/azure_sql_to_gcs.py` processa as tabelas habilitadas em `config/gcs_exports.yaml`. Essa configuração fica separada do código e do `.env`: você pode habilitar ou desabilitar tabelas sem alterar a lógica de extração. Neste projeto, `columns: "*"` leva todas as colunas; antes de usar dados pessoais ou produção, avalie minimização e classificação LGPD.

Instale as dependências e configure a autenticação local do Google Cloud:

```powershell
python -m pip install -r requirements.txt
gcloud auth application-default login
```

A identidade autenticada precisa ter permissão para criar objetos no bucket. O cliente usa Application Default Credentials; não armazene uma chave JSON do Google no `.env` ou no repositório.

Configure `GCS_BUCKET` no `.env` com o nome do bucket existente e use `GCS_PREFIX=raw`. O script acrescenta o nome da tabela configurado no YAML, produzindo caminhos como `raw/order_items/extract_date=.../run_id=.../`. Para cada tabela, edite o YAML e use `enabled: true` para incluir ou `enabled: false` para ignorar. `chunk_rows` pode ser configurado por tabela. O valor padrão vem de `EXPORT_CHUNK_ROWS` no `.env`.

Execute da raiz do projeto:

```powershell
python -m pipelines.azure_sql_to_gcs
```

Cada execução usa um `run_id` novo, impresso ao final. Se falhar, os Parquet já enviados permanecem no GCS, mas sem `_RUN_SUCCESS.json` não devem ser carregados.

## Carregar os Parquet no BigQuery

O carregador exige `_RUN_SUCCESS.json` e `_SUCCESS.json` e confere os objetos Parquet registrados no manifesto. Carrega cada tabela em uma tabela separada por execução no dataset de staging, reconcilia as linhas e só então promove para o dataset Bronze. As tabelas temporárias expiram após `BQ_STAGING_TABLE_TTL_DAYS` (padrão: 7). `_BIGQUERY_RUN_SUCCESS.json` indica que todas as tabelas foram promovidas.

O pipeline `pipelines/gcs_parquet_to_bigquery.py` usa load jobs nativos do BigQuery. O BigQuery lê o schema diretamente dos arquivos Parquet.

Os datasets de staging e Bronze precisam existir antes da carga e ficar na mesma localização do bucket. Configure `BQ_STAGING_DATASET`, `BQ_BRONZE_DATASET`, `BQ_LOCATION` e `BQ_STAGING_TABLE_TTL_DAYS` no `.env`. Crie os dois datasets uma vez (substitua `<project-id>` pelo valor de `GCP_PROJECT_ID`):

```powershell
bq --location=us-central1 mk --dataset <project-id>:PROJECT_1_STAGING
bq --location=us-central1 mk --dataset <project-id>:PROJECT_1_BRONZE
```

O ID do projeto e os datasets são configuração de ambiente, sem credenciais versionadas. A identidade local precisa de permissão para criar load jobs, criar/atualizar tabelas nos dois datasets e ler/listar os objetos no bucket.

Execute a extração e aguarde a mensagem de conclusão para todas as tabelas. Copie o `run_id` impresso e carregue exatamente essa execução:

```powershell
python -m pipelines.gcs_parquet_to_bigquery --run-id 20261009T172516Z-aeb16ae6
```

O dataset Bronze recebe o snapshot full promovido. O dataset staging contém tabelas temporárias separadas por execução; não use um wildcard que cubra todos os `run_id`. O código valida a localização dos dois datasets e do bucket antes dos jobs.

### Atualizar somente tabelas `full_refresh`

Depois do baseline inicial, não execute o full geral como rotina: ele também contém as tabelas CDC e estabelece um novo baseline. Para atualizar somente as tabelas marcadas como `full_refresh` no YAML, use a mesma implementação com a estratégia seletiva. Isso mantém as tabelas CDC e seus checkpoints intactos:

```powershell
python -m pipelines.azure_sql_to_gcs --strategy full_refresh
$runId = Read-Host "RUN_ID impresso pela extração"
python -m pipelines.gcs_parquet_to_bigquery --strategy full_refresh --run-id $runId
```

O extrator e o carregador validam que o manifesto pertence ao modo `full_refresh`; o carregador não inicializa nem altera `_cdc_checkpoints` nesse modo. O `RUN_ID` continua representando uma execução seletiva, com manifesto e reconciliação próprios. Não use `--reset-cdc-checkpoints` nessa atualização.

## Transformar Bronze em Silver com dbt

O projeto dbt fica em `dbt/`, separado das dependências de ingestão. Ele declara
as tabelas Bronze como `sources`, cria views de staging em
`PROJECT_1_DEV_SILVER` durante o desenvolvimento e testa chaves primárias e
relacionamentos entre itens, pedidos e produtos. As
tabelas de clientes, contatos, endereços e funcionários ficam declaradas como
sources, mas não são copiadas para Silver nesta primeira etapa; avalie finalidade,
minimização e acesso antes de modelar dados potencialmente pessoais.

O adaptador BigQuery do dbt v1 ainda não oferece suporte ao Python 3.14. Use um
ambiente separado com Python 3.13 para não alterar o ambiente que executa os
pipelines Python:

```powershell
py -3.13 -m venv .venv-dbt
.\.venv-dbt\Scripts\Activate.ps1
python -m pip install -r requirements-dbt.txt
gcloud auth application-default login
```

O `profiles.yml` usa OAuth/ADC e variáveis de ambiente, sem chave de serviço no
repositório. No PowerShell, configure as mesmas referências do `.env` para a
sessão dbt:

```powershell
$env:GCP_PROJECT_ID = "<seu-project-id>"
$env:BQ_BRONZE_DATASET = "PROJECT_1_BRONZE"
$env:DBT_DEFAULT_DATASET = "PROJECT_1_DEV"
$env:BQ_LOCATION = "us-central1"
```

Crie os datasets Silver e Gold de desenvolvimento na mesma região da Bronze e
execute da raiz do repositório:

```powershell
bq --location=us-central1 mk --dataset "${env:GCP_PROJECT_ID}:PROJECT_1_DEV_SILVER"
bq --location=us-central1 mk --dataset "${env:GCP_PROJECT_ID}:PROJECT_1_DEV_GOLD"
dbt debug --project-dir dbt --profiles-dir dbt
dbt build --project-dir dbt --profiles-dir dbt
```

`dbt build` compila e executa os modelos e os testes declarados. As fontes usam
`source()` e os modelos downstream usam `ref()`, permitindo ao dbt organizar a
linhagem e a ordem de execução. Os modelos staging/intermediate ficam em
`PROJECT_1_DEV_SILVER` e os marts em `PROJECT_1_DEV_GOLD` durante o desenvolvimento;
as transformações de negócio e modelos de fatos/dimensões serão uma próxima
etapa após inspecionarmos as colunas e regras das tabelas.

### Verificar a conexão com Azure SQL

Execute o exemplo a partir da raiz do repositório:

```powershell
python -m scripts.check_azure_sql_connection
```

O script abre uma conexão, executa `SELECT 1` sem ler dados das tabelas e fecha os recursos. Para usar a
mesma conexão em outro módulo:

```python
from sqlalchemy import text

from src.azure_sql import get_engine

engine = get_engine()
try:
    with engine.connect() as connection:
        rows = connection.execute(text("SELECT 1 AS exemplo"))
        print(rows.scalar_one())
finally:
    engine.dispose()
```

Use `text()` para consultas parametrizadas do SQLAlchemy e não concatene valores
de entrada diretamente em SQL.

### Inspecionar CDC para planejar o incremental

Antes de extrair mudanças, consulte quais tabelas estão habilitadas no CDC, os capture instances, o intervalo de LSN retido, a retenção configurada e as chaves primárias/únicas:

```powershell
python -m scripts.inspect_azure_sql_cdc
```

Esse comando é somente leitura e mostra metadados, não os registros das tabelas. Use o relatório para confirmar se todas as tabelas de `config/gcs_exports.yaml` estão sendo capturadas e se há uma chave utilizável para aplicar inserções, atualizações e exclusões.

O YAML marca `order_items`, `orders` e `products` com `incremental_strategy: cdc`; as demais tabelas usam `full_refresh`. O arquivo SQL habilita CDC somente para as três tabelas CDC e ignora as que já estão capturadas. Execute-o manualmente na base de origem com uma conta autorizada, não com a credencial somente leitura do pipeline; depois, confirme com `python -m scripts.inspect_azure_sql_cdc` que as três aparecem como `TRACKED`.

### Bootstrap do baseline CDC

O pipeline continua fazendo uma carga full de todas as tabelas habilitadas. Antes de abrir a transação `SNAPSHOT`, ele lê `sys.fn_cdc_get_max_lsn()` e valida a capture instance, o LSN inicial e o limite mínimo retido de cada tabela configurada para CDC. O LSN de referência e esses metadados são gravados nos manifestos `_SUCCESS.json` e `_RUN_SUCCESS.json`. Depois de promover o full, o carregador inicializa `PROJECT_1_BRONZE._cdc_checkpoints`. Se já houver um checkpoint diferente, a promoção falha para evitar reiniciar o estado incremental silenciosamente.

Com as três tabelas rastreadas, execute um novo full e carregue exatamente o `RUN_ID` impresso:

```powershell
python -m pipelines.azure_sql_to_gcs
$runId = Read-Host "Informe o RUN_ID impresso pela extração"
python -m pipelines.gcs_parquet_to_bigquery --run-id $runId
```

O watermark é capturado antes do snapshot. Mudanças confirmadas depois dele podem também estar refletidas no snapshot; a aplicação incremental é idempotente. Não inicie um segundo baseline concorrente; conclua o carregamento BigQuery e guarde o `RUN_ID` e os manifestos de sucesso.

### Executar o incremental CDC

`config/gcs_exports.yaml` declara a chave primária de cada tabela CDC. O extrator `pipelines/azure_sql_cdc_to_gcs.py` lê o checkpoint no BigQuery, valida capture instance e retenção e extrai `all_changes` em chunks Parquet. Isso preserva eventos de inserção, atualização e exclusão no GCS. O aplicador `pipelines/gcs_cdc_to_bigquery.py` carrega o lote em staging, registra os eventos em uma tabela `_cdc_history_<tabela>` particionada pelo horário de ingestão e agrupada pela chave e aplica o último evento de cada chave à tabela Bronze corrente. Histórico, Bronze e checkpoint são atualizados numa transação BigQuery; reprocessar o mesmo `RUN_ID` não duplica eventos nem avança o checkpoint duas vezes.

As tabelas `_cdc_history_*` e os Parquet CDC conservam os dados das alterações, inclusive valores de linhas excluídas. Defina a retenção e o processo de atendimento a solicitações de eliminação antes de usar dados pessoais reais; configure também a política de ciclo de vida do bucket.

Como o código atual já carregou o baseline, execute novamente o carregador full com o mesmo `RUN_ID` para inicializar o checkpoint durável (ele falha se o checkpoint já existir em outro LSN):

```powershell
$baselineRunId = Read-Host "RUN_ID do full cujo manifesto contém os LSN"
python -m pipelines.gcs_parquet_to_bigquery --run-id $baselineRunId
```

Depois execute e aplique um lote CDC:

```powershell
python -m pipelines.azure_sql_cdc_to_gcs
$cdcRunId = Read-Host "CDC_RUN_ID impresso pela extração"
python -m pipelines.gcs_cdc_to_bigquery --run-id $cdcRunId
```

Execute extração e aplicação sequencialmente, sem runs concorrentes para as mesmas tabelas. Uma falha de retenção indica que o checkpoint ficou anterior ao `min_lsn`; nesse caso, pare o incremental, gere um novo full e carregue-o com `--reset-cdc-checkpoints` para recomeçar intencionalmente o baseline. Sem essa opção, um full cujo LSN não corresponda ao checkpoint existente falha antes de substituir as tabelas Bronze. O `min_lsn` é o limite inferior atualmente disponível para a capture instance. As funções CDC incluem os limites `from_lsn` e `to_lsn`; por isso, o extrator começa com `sys.fn_cdc_increment_lsn(last_lsn)`. [Documentação da Microsoft sobre CDC e intervalos LSN](https://learn.microsoft.com/en-us/sql/relational-databases/track-changes/work-with-change-data-sql-server?view=sql-server-ver17)

## Segurança

- Mantenha criptografia TLS habilitada e validação do certificado ativa.
- Não registre senhas, tokens ou dados pessoais nos logs.
- Antes de extrair dados pessoais, confirme a finalidade, a autorização e os
  campos necessários para o exercício.
