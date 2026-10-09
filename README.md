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

O script `examples/export_order_items_to_gcs.py` processa as tabelas habilitadas em `config/gcs_exports.yaml`. Essa configuração fica separada do código e do `.env`: você pode habilitar ou desabilitar tabelas sem alterar a lógica de extração.

Instale as dependências e configure a autenticação local do Google Cloud:

```powershell
python -m pip install -r requirements.txt
gcloud auth application-default login
```

A identidade autenticada precisa ter permissão para criar objetos no bucket. O cliente usa Application Default Credentials; não armazene uma chave JSON do Google no `.env` ou no repositório.

Configure `GCS_BUCKET` no `.env` com o nome do bucket existente. Para cada tabela, edite o YAML e use `enabled: true` para incluir ou `enabled: false` para ignorar. Prefira listar somente as colunas necessárias em `columns`; `"*"` deve ser usado apenas durante exploração. `chunk_rows` pode ser configurado por tabela. O valor padrão vem de `EXPORT_CHUNK_ROWS` no `.env`.

Execute da raiz do projeto:

```powershell
python -m examples.export_order_items_to_gcs
```

Cada execução usa um `run_id` novo. Se falhar durante uma tabela, arquivos já enviados permanecem na pasta daquela execução; não trate uma execução parcial como concluída.

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
