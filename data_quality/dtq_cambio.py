import pandas as pd
import sys
sys.path.append('../utils_db_ebanx')
from fn_database import conectar
from fn_database import executar_consulta


inconsistencias = 0

# Meses nulos

conexao = conectar('bronze')


cambio =  executar_consulta(conexao,''' 
    SELECT *
    FROM raw_cambio
    WHERE "Mês" IS NULL
''')

if cambio.shape[0] >= 1:
    print("Há meses nulos")
    inconsistencias = inconsistencias + 1



# Meses duplicados

cambio =  executar_consulta(conexao,''' 
    SELECT Mês, COUNT(1)
    FROM raw_cambio
    GROUP BY Mês
    HAVING COUNT(1) > 1
''')

if cambio.shape[0] >= 1:
    print("Há meses duplicados")
    inconsistencias = inconsistencias + 1



# Cambio vazio
cambio =  executar_consulta(conexao,''' 
   SELECT BRL, EUR, CNY, EGP, KRW, CLP, MXN
    FROM raw_cambio
    WHERE 
		BRL IS NULL OR BRL <= 0
        OR EUR IS NULL OR EUR <= 0
        OR CNY IS NULL OR CNY <= 0
        OR EGP IS NULL OR EGP <= 0
        OR KRW IS NULL OR KRW <= 0
        OR MXN IS NULL OR MXN <= 0
''')

if cambio.shape[0] >= 1:
    print("Há cambio sem valor") 
    inconsistencias = inconsistencias + 1



if inconsistencias == 0:
    print("Não há inconsistências na base raw_cambio")

print("Processo de Data Quality finalizado")