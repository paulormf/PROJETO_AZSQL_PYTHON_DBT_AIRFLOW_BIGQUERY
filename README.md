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

O exemplo `examples/export_order_items_to_gcs.py` lê a tabela em lotes, grava
um arquivo Parquet por lote e envia cada arquivo ao Cloud Storage. A memória
fica limitada ao lote atual e o arquivo temporário é removido após o upload.
Cada execução grava em um prefixo novo com data e identificador da execução,
sem sobrescrever arquivos de execuções anteriores.

Instale as dependências e configure a autenticação local do Google Cloud:

```powershell
python -m pip install -r requirements.txt
gcloud auth application-default login
```

O usuário autenticado precisa ter permissão para criar objetos no bucket, como
`Storage Object User` concedida no bucket. A biblioteca usa Application Default
Credentials; não salve chave JSON do Google em `.env` nem no repositório.

Adicione ao `.env` as variáveis de exportação do `.env.example` e substitua
`GCS_BUCKET` pelo nome do bucket existente. `AZURE_SQL_COLUMNS=*` é conveniente
para a primeira exploração; para uma extração governada, informe apenas as
colunas necessárias, por exemplo `AZURE_SQL_COLUMNS=order_id,product_id,quantity`.
Confirme também o schema real de `order_items`.

Execute da raiz do projeto:

```powershell
python -m examples.export_order_items_to_gcs
```

`EXPORT_CHUNK_ROWS` controla quantas linhas são processadas de cada vez. O valor
inicial é 50.000; ajuste com base no tamanho das linhas e na memória disponível.
Se houver falha durante uma execução, os arquivos já enviados ficam sob o
`run_id` daquela execução. Não trate uma pasta parcial como carga completa.

### Exemplo de uso

Execute o exemplo a partir da raiz do repositório:

```powershell
python -m examples.azure_sql_connection_example
```

O exemplo abre uma conexão, executa `SELECT 1` e fecha os recursos. Para usar a
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

## Segurança

- Mantenha criptografia TLS habilitada e validação do certificado ativa.
- Não registre senhas, tokens ou dados pessoais nos logs.
- Antes de extrair dados pessoais, confirme a finalidade, a autorização e os
  campos necessários para o exercício.
