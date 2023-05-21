import pandas as pd
import sys
sys.path.append('../utils_db_ebanx')
from fn_database import conectar
from fn_database import executar_consulta


inconsistencias = 0

#---------------------------------- VALORES DUPLICADOS

# id compra duplicada

conexao = conectar('bronze')


compra =  executar_consulta(conexao,''' 
    SELECT id, COUNT(1)
    FROM bronze.raw_compras
    GROUP BY id
    HAVING COUNT(1) > 1
''')

if compra.shape[0] >= 1:
    print("Há id de compras duplicadas")
    inconsistencias = inconsistencias + 1


#---------------------------------- VALORES DUPLICADOS


# id nulo

compra =  executar_consulta(conexao,''' 
    SELECT * 
    FROM bronze.raw_compras
    WHERE id IS NULL
''')

if compra.shape[0] >= 1:
    print("Há id nulo")
    inconsistencias = inconsistencias + 1


# id_cliente nulo

compra =  executar_consulta(conexao,''' 
    SELECT * 
    FROM bronze.raw_compras
    WHERE id_cliente IS NULL
''')

if compra.shape[0] >= 1:
    print("Há id_cliente nulo")
    inconsistencias = inconsistencias + 1


# data_vencimento nulo

compra =  executar_consulta(conexao,''' 
    SELECT * 
    FROM bronze.raw_compras
    WHERE data_vencimento IS NULL
''')

if compra.shape[0] >= 1:
    print("Há data_vencimento nulo")
    inconsistencias = inconsistencias + 1
    

# valor nulo

compra =  executar_consulta(conexao,''' 
    SELECT * 
    FROM bronze.raw_compras
    WHERE valor IS NULL OR valor <=0
''')

if compra.shape[0] >= 1:
    print("Há valor nulo ou menor que 0")
    inconsistencias = inconsistencias + 1


# categoria nula

compra =  executar_consulta(conexao,''' 
    SELECT * 
    FROM bronze.raw_compras
    WHERE categoria IS NULL
''')

if compra.shape[0] >= 1:
    print("Há categoria nula")
    inconsistencias = inconsistencias + 1


# moeda nula

compra =  executar_consulta(conexao,''' 
    SELECT * 
    FROM bronze.raw_compras
    WHERE moeda IS NULL
''')

if compra.shape[0] >= 1:
    print("Há moeda nula")
    inconsistencias = inconsistencias + 1


# status nulo

compra =  executar_consulta(conexao,''' 
    SELECT * 
    FROM bronze.raw_compras
    WHERE status IS NULL
''')

if compra.shape[0] >= 1:
    print("Há status nulo")
    inconsistencias = inconsistencias + 1



#---------------------------------- VALORES INESPERADOS

#---- Categoria nova registrada

compra =  executar_consulta(conexao,''' 
    SELECT distinct categoria
    FROM bronze.raw_compras
    WHERE categoria not in ('Cosméticos'
    ,'Decoração'
    ,'Eletrônicos'
    ,'Perfumaria'
    ,'Vestuário')
''')

if compra.shape[0] >= 1:
    print("Há categoria inesperada registrada")
    inconsistencias = inconsistencias + 1


#---- Moeda nova registrada

compra =  executar_consulta(conexao,''' 
    SELECT distinct moeda
    FROM bronze.raw_compras
    WHERE moeda not in (
        'BRL','CLP','CNY','EGP'
        ,'EUR','KRW','MXN','USD')
''')

if compra.shape[0] >= 1:
    print("Há moeda inesperada registrada")
    inconsistencias = inconsistencias + 1


#---- Status novo registrada

compra =  executar_consulta(conexao,''' 
    SELECT distinct status
    FROM bronze.raw_compras
    WHERE status not in (
        'Pago', 'Pendente')
''')

if compra.shape[0] >= 1:
    print("Há status inesperado registrado")
    inconsistencias = inconsistencias + 1



#---------------------------------- REGRAS DE PAGAMENTO

#--- Pagamento confirmado mas data de pagamento nula


compra =  executar_consulta(conexao,''' 
    SELECT *
    FROM bronze.raw_compras
    WHERE status = 'Pago' AND data_pagamento IS NULL
''')

if compra.shape[0] >= 1:
    print("Há pagamento registrado sem data de pagamento registrada")
    inconsistencias = inconsistencias + 1


#--- Pagamento pendente mas com data de pagamento registrada


compra =  executar_consulta(conexao,''' 
    SELECT *
    FROM bronze.raw_compras
    WHERE status = 'Pendente' AND data_pagamento IS NOT NULL
''')

if compra.shape[0] >= 1:
    print("Há pagamento pendente com data de pagamento registrada")
    inconsistencias = inconsistencias + 1



if inconsistencias == 0:
    print("Não há inconsistências na base raw_compras")

print("Processo de Data Quality finalizado")

