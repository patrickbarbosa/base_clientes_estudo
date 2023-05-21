import pandas as pd
import sys
sys.path.append('../utils_db_ebanx')
from fn_database import conectar
from fn_database import executar_consulta
from fn_database import inserir_dados


#Conexão
conexao = conectar('silver')
tb_dm_clientes =  executar_consulta(conexao,'''
    SELECT * FROM stg_cambio
''')

conexao = conectar('gold')
inserir_dados(conexao, 'tb_dm_cambio', [f"`{col}` VARCHAR(255)" for col in tb_dm_clientes.columns], dados = tb_dm_clientes.values.tolist())