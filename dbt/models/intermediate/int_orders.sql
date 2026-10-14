with total_orders_items as (
    
    select 
    order_id, 
    sum(total_item) as item_total_amount
    from {{ ref('stg_order_items') }} 
    group by order_id
)

select 

    o.order_id,
    o.customer_id,
    FORMAT_DATETIME('%d/%m/%Y %H:%M:%S', DATETIME(o.order_date, 'America/Sao_Paulo')) AS order_date,
    o.discount as discount,
    o.tax as tax,
    total_orders_items.item_total_amount,
    case
        when o.payment_method = 'CREDIT_CARD' then 'cartão de crédito'
        when o.payment_method = 'DEBIT_CARD' then 'cartão de débito'
        when o.payment_method = 'BANK_TRANSFER' then 'transferência bancária'
        when o.payment_method = 'BOLETO' then 'boleto'
        when o.payment_method = 'PIX' then 'pix'
        else 'desconhecido'
    end as payment_method,
    case 
        when o.status = 'CANCELLED' then 'cancelado'
        when o.status = 'DELIVERED' then 'entregue'
        when o.status = 'SHIPPED' then 'enviado'
        when o.status = 'PROCESSING' then 'processando'
        when o.status = 'PENDING' then 'pendente'
        when o.status = 'PAID' then 'pago'
        else 'desconhecido'
    end as order_status,

from {{ ref('stg_orders') }} as o

left join total_orders_items
    on o.order_id = total_orders_items.order_id

order by o.order_id
