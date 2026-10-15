# Airflow local (Docker Compose)

Este ambiente é para desenvolvimento local no Windows com Docker Desktop e Linux containers. O Airflow roda em containers; o código dos pipelines e o dbt são executados em ambientes Python isolados dentro da imagem: ingestão e dbt não compartilham dependências.

O arquivo `.env` do projeto e o ADC do Google são montados somente no scheduler, que executa as tarefas com `LocalExecutor`; eles não são copiados para a imagem nem disponibilizados aos serviços web e de parsing das DAGs.

## Preparação

1. Crie `airflow/.env` a partir de `airflow/.env.example` (`Copy-Item airflow/.env.example airflow/.env`) e preencha valores locais exclusivos. Esse arquivo é ignorado pelo Git.
2. Confirme que o `.env` da raiz contém as configurações Azure/GCP usadas pelos pipelines.
3. Execute `gcloud auth application-default login` no host. O Compose monta o arquivo ADC do Windows dentro dos containers.
4. No Docker Desktop, use Linux containers e reserve pelo menos 4 GB de memória para Docker.

## Inicializar e iniciar

Na raiz do repositório, execute no PowerShell:

```powershell
docker compose --env-file airflow/.env -f airflow/docker-compose.yml build
docker compose --env-file airflow/.env -f airflow/docker-compose.yml up airflow-init
docker compose --env-file airflow/.env -f airflow/docker-compose.yml up -d
```

Abra `http://localhost:8080`. O usuário local é `admin`; a senha aleatória gerada fica no arquivo local `airflow/config/simple_auth_manager_passwords.json`. Consulte esse arquivo para entrar. Ele fica fora do Git.

## CI no GitHub Actions

O workflow `.github/workflows/ci.yml` roda em cada `push` e pull request. Ele
constroi a imagem Docker do Airflow, compila os arquivos Python, executa os
testes unitarios existentes, carrega as DAGs e valida o projeto dbt com
`dbt parse`. Essas etapas nao acessam Azure SQL, GCS ou BigQuery e nao precisam
de credenciais do ambiente.

O workflow valida o codigo, mas nao publica a imagem, implanta DAGs nem dispara
cargas de dados. Para bloquear merges quando a validacao falhar, configure a
protecao da branch principal no GitHub e exija o status `Validate project`.

## Fluxos

- `azure_sql_cdc_to_bigquery_dbt`: diariamente às 03:00 em `America/Sao_Paulo`, extrai CDC, aplica o lote no Bronze e executa `dbt build`.
- `azure_sql_full_refresh_to_bigquery_dbt`: semanalmente aos domingos às 04:00 em `America/Sao_Paulo`, atualiza apenas as tabelas `full_refresh`, preserva checkpoints CDC e executa `dbt build`.

Os DAGs passam o `RUN_ID` entre tarefas pelo XCom do Airflow e só avançam quando a tarefa anterior termina sem erro. `max_active_runs=1` impede duas execuções simultâneas do mesmo DAG. Antes de habilitar os horários automáticos, teste manualmente cada DAG na interface do Airflow e compare os manifestos e as tabelas no BigQuery.

Os dois DAGs são criados pausados. Faça primeiro um disparo manual de cada um na UI; depois confira os logs e os resultados no BigQuery. Despause cada DAG quando estiver satisfeito com o teste. O pool `pipeline_mutations`, criado na inicialização, serializa as cargas no Bronze e os builds dbt entre os dois DAGs.

Para parar os serviços sem apagar o banco de metadados:

```powershell
docker compose --env-file airflow/.env -f airflow/docker-compose.yml down
```

Este Compose é ambiente de aprendizado local. Em produção, prefira um serviço gerenciado de Airflow; configure conexões/secrets pela plataforma e execute as tarefas em workers ou serviços de execução próprios.
