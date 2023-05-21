import pandas as pd
import sys
sys.path.append('../utils_db_ebanx')
from fn_database import conectar
from fn_database import executar_consulta

inconsistencias = 0


# ids duplicados
conexao = conectar('bronze')


clientes =  executar_consulta(conexao,''' 
    SELECT id, COUNT(1)
    FROM raw_clientes
    GROUP BY id
    HAVING COUNT(1) > 1
''')

if clientes.shape[0] >= 1:
    print("Há clientes duplicados")
    inconsistencias = inconsistencias + 1



    
# Nomes vazios
clientes =  executar_consulta(conexao,''' 
    SELECT id
    FROM raw_clientes
    WHERE name IS NULL
''')

if clientes.shape[0] >= 1:
    print("Há nomes vazios")
    inconsistencias = inconsistencias + 1


# Datas vazias
clientes =  executar_consulta(conexao,''' 
    SELECT id
    FROM raw_clientes
    WHERE "data nascimento" IS NULL
''')

if clientes.shape[0] >= 1:
    print("Há datas vazias") 
    inconsistencias = inconsistencias + 1



if inconsistencias == 0:
    print("Não há inconsistências na base raw_clientes")


print("Processo de Data Quality finalizado")