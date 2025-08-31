-- Trazer a performance por vendeddor.
-- Porém acredito que a performance não pode ser medida somente pela quantidade de produtos vendidas e sim em um contexto geral.
-- Quantidade de vendas e valor de vendas?
-- Qual vendedor mais conseguiu evitar desconto na compra?
-- Como o exercicio pede para verificar as vendas do ultimo ano e a tabela vai somente até 2022, então eu trouxe somente o ano da data maxima da tabela.

with cte_vendas_empregados as 
(
select 
	vendas.order_id,
	detalhe_venda.product_id,
	vendas.employee_id,
	empregado.first_name,
	detalhe_venda.unit_price sale_price,
	detalhe_venda.quantity
	from orders vendas 
left join employees empregado on vendas.employee_id  = empregado.employee_id 
left join order_details detalhe_venda on vendas.order_id = detalhe_venda.order_id
where date_part('year', order_date) = ( select date_part('year', max(order_date)) from orders )
)
,
cte_vendas as
(
	select
		vendas.*,
		produtos.product_id,
		produtos.product_name,
		produtos.unit_price
	from products produtos
	inner join cte_vendas_empregados vendas on produtos.product_id = vendas.product_id
)
select
	first_name,
	sum(unit_price) as valor_total,
	sum(sale_price) - sum(unit_price) discount,
	(sum(unit_price) - sum(sale_price)) / sum(unit_price) percentual_discount,
	sum(quantity) as total_sales
	from cte_vendas
	group by first_name
	order by total_sales desc 
	;