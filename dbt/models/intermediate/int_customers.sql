select

customer_id,
customer_type,
full_name,
cpf,
cnpj,
email,
phone,
birth_date,
gender,
status

from 

{{ ref('stg_customers') }}