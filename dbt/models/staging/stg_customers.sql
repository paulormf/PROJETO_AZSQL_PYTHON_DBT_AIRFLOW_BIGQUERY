with source_customers as (
    select
        customer_id,
        customer_type,
        full_name,
        regexp_replace(cast(cpf as string), r'[^0-9]', '') as cpf_digits,
        regexp_replace(cast(cnpj as string), r'[^0-9]', '') as cnpj_digits,
        cast(email as string) as email,
        split(cast(email as string), '@')[safe_offset(0)] as email_local_part,
        split(cast(email as string), '@')[safe_offset(1)] as email_domain,
        regexp_extract(cast(email as string), r'(\.[^.@]+)$') as email_domain_suffix,
        regexp_replace(cast(phone as string), r'[^0-9]', '') as phone_digits,
        birth_date,
        gender,
        status
    from {{ source('azure_sql_bronze', 'customers') }}
)

select
    customer_id,
    customer_type,
    full_name,
    case
        when cpf_digits is null then null
        when length(cpf_digits) <= 4 then repeat('*', length(cpf_digits))
        else concat(repeat('*', length(cpf_digits) - 4), right(cpf_digits, 4))
    end as cpf,
    case
        when cnpj_digits is null then null
        when length(cnpj_digits) <= 4 then repeat('*', length(cnpj_digits))
        else concat(repeat('*', length(cnpj_digits) - 4), right(cnpj_digits, 4))
    end as cnpj,
    case
        when email is null then null
        when not regexp_contains(email, r'^[^@]+@[^@]+\.[^.@]+$') then '[REDACTED]'
        else concat(
            substr(email_local_part, 1, 2),
            repeat('*', greatest(length(email_local_part) - 2, 0)),
            '@',
            substr(email_domain, 1, 1),
            repeat(
                '*',
                greatest(length(email_domain) - 1 - length(email_domain_suffix), 0)
            ),
            email_domain_suffix
        )
    end as email,
    case
        when phone_digits is null then null
        when length(phone_digits) <= 4 then repeat('*', length(phone_digits))
        else concat(repeat('*', length(phone_digits) - 4), right(phone_digits, 4))
    end as phone,
    birth_date,
    gender,
    status
from source_customers
