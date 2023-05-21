/*

1)

Como identificar os clientes que realizaram as compras com os maiores valores
em dólares e qual a nacionalidade desses clientes?

 Interpretações:  
	Compras realizadas, independentemente se realizou pagamento.
	Compras PAGAS realizadas.

*/

SELECT A.id_venda,A.id_cliente, A.nome_cliente, A.pais_cliente, A.valor_dolar
 FROM gold.tb_fato_compras A
 ORDER BY valor_dolar DESC;
 
 
 SELECT A.id_venda,A.id_cliente, A.nome_cliente, A.pais_cliente, A.valor_dolar
 FROM gold.tb_fato_compras A
 WHERE A.status = 'Pago'
 ORDER BY valor_dolar DESC;
 

 /*

 2)

 Identifique a categoria de produto com maior número de pagamentos em atraso.
 
 Interpretações:
	Vendas que POSSUEM pagamentos e que foram concluídas com atraso.
	Vendas que POSSUEM pagamentos OU estão PENDENTES.
 
 */
 
 SELECT categoria, COUNT(DISTINCT id_venda) AS Vendas
 FROM gold.tb_fato_compras A
 WHERE A.flg_Pago_Vencido = '1'
 GROUP BY categoria;
 
 
 SELECT categoria, COUNT(DISTINCT id_venda) AS Vendas
 FROM gold.tb_fato_compras A
 WHERE A.flg_Pago_Vencido = '1' OR A.status = 'Pendente'
 GROUP BY categoria;

 
 /*
 3)

	 Considerando uma multa de 0,25 dólares por dia de atraso, como a lista de
	clientes e categorias ficaria ordenada em relação aos atrasos nos pagamentos?
	
    Interpretações:
		Todas as compras
        Todas as compras PAGAS.
 */
 
 
 SELECT A.id_venda,A.id_cliente, A.nome_cliente, A.pais_cliente,A.categoria, A.valor_total_dolar
 FROM gold.tb_fato_compras A
 ORDER BY valor_total_dolar DESC;	
 
SELECT A.id_venda,A.id_cliente, A.nome_cliente, A.pais_cliente,A.categoria, A.valor_total_dolar
 FROM gold.tb_fato_compras A
 WHERE A.status	= 'Pago'
 ORDER BY valor_total_dolar DESC;	
 
#----------------- 3.2) Categorias

# Compras pagas ANTES
SELECT A.categoria, SUM(valor_dolar)
FROM gold.tb_fato_compras A
WHERE A.status	= 'Pago'
GROUP BY A.categoria
ORDER BY 2 DESC;

# Compras pagas DEPOIS
SELECT A.categoria, SUM(valor_total_dolar)
FROM gold.tb_fato_compras A
WHERE A.status	= 'Pago'
GROUP BY A.categoria
ORDER BY 2 DESC;

#---------
# Todas as Compras 	ANTES (Inclusive oq não foi pago)
SELECT A.categoria, SUM(valor_dolar)
FROM gold.tb_fato_compras A
GROUP BY A.categoria
ORDER BY 2 DESC;

# Todas as Compras 	DEPOIS (Inclusive oq não foi pago)
SELECT A.categoria, SUM(valor_total_dolar)
FROM gold.tb_fato_compras A
GROUP BY A.categoria
ORDER BY 2 DESC;




 /*
 4)

 Entre os pagamentos que ainda estão pendentes, qual é a categoria que tem o
maior valor estimado convertido para dólar?

*/


SELECT categoria, ROUND(SUM(valor_total_dolar),2) AS Valor
FROM gold.tb_fato_compras A
WHERE A.status = 'Pendente'
GROUP BY categoria
ORDER BY 2 DESC;
 
 /*
 5)

 Gerar uma lista contendo o nome do cliente e um campo com todas as
categorias de produto que ele comprou separados por vírgula.

 */
 
SELECT A.id_cliente, A.nome_cliente, GROUP_CONCAT(DISTINCT A.categoria SEPARATOR ', ') AS categorias
FROM gold.tb_fato_compras A
GROUP BY A.id_cliente, A.nome_cliente;
