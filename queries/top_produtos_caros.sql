-- Exercicio para trazer as top 10 vendas

select
	product_name,
	sum(unit_price) unit_price 
from products
group by product_name
order by unit_price desc
limit 10