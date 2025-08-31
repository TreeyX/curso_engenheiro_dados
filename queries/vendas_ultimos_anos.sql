-- Diferença das vendas dos ultimos dois anos por fornecedor

with cte_atividades_vendas as 
(
select
	detalhes_vendas.order_id, 
	produtos.product_id,
	produtos.supplier_id,
	produtos.product_name,
	fornecedores.company_name,
	vendas.order_date,
	produtos.unit_price,
	detalhes_vendas.unit_price unit_amt,
	(detalhes_vendas.unit_price * detalhes_vendas.quantity) sale_amt,
	detalhes_vendas.quantity
	from products produtos
inner join order_details detalhes_vendas on produtos.product_id = detalhes_vendas.product_id
left join orders vendas on detalhes_vendas.order_id  = vendas.order_id 
left join suppliers fornecedores on produtos.supplier_id = 	fornecedores.supplier_id
),
cte_vendas_last_year as
(
select
	company_name,
	sum(sale_amt) as last_yr_amt
from cte_atividades_vendas 
where date_part('year', order_date) = ( select date_part('year', max(order_date)) from orders ) - 2
group by company_name
),
cte_vendas_curr_year as
(
select
	company_name,
	sum(sale_amt) as curr_yr_amt
from cte_atividades_vendas 
where date_part('year', order_date) = ( select date_part('year', max(order_date)) from orders ) - 1
group by company_name
) 
 select 
 	lst_yr.company_name,
 	lst_yr.last_yr_amt,
 	curr_yr.curr_yr_amt,
 	curr_yr.curr_yr_amt - lst_yr.last_yr_amt dff_amt
 from cte_vendas_last_year lst_yr
 left join cte_vendas_curr_year curr_yr on lst_yr.company_name = curr_yr.company_name ;