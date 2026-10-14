select
    oi.order_item_id,
    oi.order_id,
    o.customer_id,
    oi.product_id,
    o.order_date as order_timestamp,
    date(o.order_date, 'America/Sao_Paulo') as order_date,
    cast(
        format_date(
            '%Y%m%d',
            date(o.order_date, 'America/Sao_Paulo')
        ) as int64
    ) as order_date_key,
    o.payment_method,
    o.status as order_status,
    oi.total_item as item_sales_amount
from {{ ref('stg_order_items') }} as oi
left join {{ ref('stg_orders') }} as o
    on oi.order_id = o.order_id
