import pandas as pd

import sys
sys.path.append('../utils_db_ebanx')
from fn_database import conectar
from fn_database import executar_consulta
from fn_database import inserir_dados


#Conexão
conexao = conectar('silver')

# Adicionando colunas auxiliares
tb_fato_compras = executar_consulta(conexao,'''
    WITH CTE AS (
    SELECT A.id_venda, A.id_cliente, A.data_vencimento, A.data_pagamento, A.valor, A.categoria, B.moeda_cambio, A.flg_pago_vencido, A.dias_atraso, A.status
        ,C.nome_cliente, C.pais_cliente
        ,CASE WHEN B.moeda_cambio <> 'USD' THEN B.valor_cambio ELSE 1 END valor_cambio
        ,CASE WHEN B.moeda_cambio <> 'USD' THEN A.valor * B.valor_cambio ELSE A.valor END AS valor_dolar
    FROM silver.stg_compras A
    LEFT JOIN silver.stg_cambio B
        ON (A.mes_Pagamento = B.num_mes AND A.moeda = B.moeda_cambio) OR (A.status = 'Pendente' AND  MONTH(current_date()) = B.num_mes AND A.moeda = B.moeda_cambio)
    LEFT JOIN silver.stg_clientes C
        ON A.id_cliente = C.id_cliente
    )
    SELECT 
        A.*
        ,CASE 
			WHEN flg_pago_vencido = 1 THEN ROUND(((valor_dolar) + dias_atraso * 0.25),2)  # Calcula juros
            WHEN (status = 'Pendente' AND CURDATE() > data_vencimento)  THEN ((valor_dolar) + ((DATEDIFF(data_vencimento,CURDATE())) * 0.25)) # Caso tenha algum pendente vencido, calcular juros de calculando a diff de dias
			ELSE valor_dolar END AS valor_total_dolar
    FROM CTE A
''')


#        --,CASE WHEN A.status = 'Pendente' THEN ROUND(((valor_dolar) + (DATEDIFF(CURDATE(),data_vencimento)) * 0.25),2) ELSE 0 END AS A



conexao = conectar('gold')
inserir_dados(conexao, 'ssot_compras', [f"`{col}` VARCHAR(255)" for col in tb_fato_compras.columns], dados = tb_fato_compras.values.tolist())

executar_consulta(conexao,'''
    ALTER TABLE tb_fato_compras
    MODIFY COLUMN valor_dolar FLOAT,
    MODIFY COLUMN valor_total_dolar FLOAT ''')

# /*
# tb_fato_compras_pendentes('''
#     WITH compras_pendentes
# '''
# )

# */