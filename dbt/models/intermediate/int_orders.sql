with order_item_totals as (
    select
        order_id,
        sum(total_item) as item_total_amount
    from {{ ref('stg_order_items') }}
    group by order_id
)

select
    o.order_id,
    o.customer_id,
    o.order_date,
    o.discount,
    o.tax,
    order_item_totals.item_total_amount,
    o.payment_method,
    o.status as order_status
from {{ ref('stg_orders') }} as o
left join order_item_totals
    on o.order_id = order_item_totals.order_id
