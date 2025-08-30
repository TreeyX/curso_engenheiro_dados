--Gerar dados que mostrem vendas com a diferenças entre o preço de tabela do produto com o preço praticado na venda

select
	produtos.product_id,
	vendas.order_id,
	produtos.product_name,
	produtos.unit_price,
	vendas.unit_price sale_price,
	vendas.quantity,
	vendas.unit_price - produtos.unit_price discount,
	(produtos.unit_price - vendas.unit_price) / produtos.unit_price percentual_discount
from products produtos
inner join order_details vendas on produtos.product_id = vendas.product_id
order by percentual_discount desc