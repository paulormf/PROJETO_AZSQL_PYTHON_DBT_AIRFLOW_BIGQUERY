select *
from {{ source('azure_sql_bronze', 'products') }}
