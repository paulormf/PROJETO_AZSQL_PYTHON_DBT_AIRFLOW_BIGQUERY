select
    p.product_id,
    p.sku,
    p.product_name,
    p.category_id,
    c.category_name,
    p.brand_id,
    p.supplier_id,
    p.unit,
    p.weight,
    p.cost_price,
    p.sale_price,
    p.tax_rate,
    p.status as product_status
from {{ ref('stg_products') }} as p
left join {{ ref('stg_categories') }} as c
    on p.category_id = c.category_id
