select *
from {{ source('azure_sql_bronze', 'order_items') }}
