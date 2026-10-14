select * replace (
    trim(initcap(category_name)) as category_name,
    lower(status) as status
)
from {{ source('azure_sql_bronze', 'categories') }}
