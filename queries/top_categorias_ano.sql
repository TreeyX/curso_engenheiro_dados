-- Listar as cinco categorias de produtos mais vendidas de cada ano

with cte_atividade_vendas as
(
select 
	detalhes_vendas.order_id,
	detalhes_vendas.product_id,
	produtos.category_id,
	categorias.category_name,
	vendas.order_date,
	date_part('year', vendas.order_date) order_year,
	detalhes_vendas.unit_price,
	detalhes_vendas.quantity,
	detalhes_vendas.unit_price * detalhes_vendas.quantity sales_amt
	from order_details detalhes_vendas
left join products produtos on detalhes_vendas.product_id  = produtos.product_id 
left join categories categorias on produtos.category_id = categorias.category_id
left join orders vendas on detalhes_vendas.order_id = vendas.order_id
),
cte_vendas_ano as
(
select
	order_year,
	category_name,
	sum(sales_amt) sales_amt
from cte_atividade_vendas
group by order_year, category_name
)
,
cte_top_categorias as 
(
select *, 
	row_number() over (partition by order_year order by order_year, sales_amt desc) as rank_sale 
from cte_vendas_ano
order by rank_sale 
)
select * 
	from cte_top_categorias 
where rank_sale <= 5
order by order_year, rank_sale ;