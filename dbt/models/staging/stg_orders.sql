select * replace (
    case
        when payment_method = 'CREDIT_CARD' then 'cartão de crédito'
        when payment_method = 'DEBIT_CARD' then 'cartão de débito'
        when payment_method = 'BANK_TRANSFER' then 'transferência bancária'
        when payment_method = 'BOLETO' then 'boleto'
        when payment_method = 'PIX' then 'pix'
        else 'desconhecido'
    end as payment_method,
    case
        when status = 'CANCELLED' then 'cancelado'
        when status = 'DELIVERED' then 'entregue'
        when status = 'SHIPPED' then 'enviado'
        when status = 'PROCESSING' then 'processando'
        when status = 'PENDING' then 'pendente'
        when status = 'PAID' then 'pago'
        else 'desconhecido'
    end as status
)
from {{ source('azure_sql_bronze', 'orders') }}
