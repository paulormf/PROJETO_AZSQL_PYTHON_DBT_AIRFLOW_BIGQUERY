select
    customer_id,
    customer_type,
    status as customer_status
from {{ ref('stg_customers') }}
