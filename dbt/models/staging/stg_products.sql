select * replace (
    trim(initcap(product_name)) as product_name,
    trim(lower(unit)) as unit,
    case
        when status = 'ACTIVE' then 'ativo'
        when status = 'INACTIVE' then 'inativo'
        else 'desconhecido'
    end as status
)
from {{ source('azure_sql_bronze', 'products') }}
