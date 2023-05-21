import pandas as pd
from datetime import datetime

import sys
sys.path.append('../utils_db_ebanx')
from fn_database import conectar
from fn_database import executar_consulta
from fn_database import inserir_dados


conexao = conectar('bronze')

raw_clientes = pd.read_excel("base.xlsx",sheet_name="clientes")
raw_compras = pd.read_excel("base.xlsx",sheet_name="compras")
raw_cambio = pd.read_excel("base.xlsx",sheet_name="cambio")


# Adicionando data de Importação
raw_clientes["import_date"] = datetime.now()
raw_compras["import_date"] = datetime.now()
raw_cambio["import_date"] = datetime.now()

# Criar as tabelas
inserir_dados(conexao, 'raw_clientes', [f"`{col}` VARCHAR(255)" for col in raw_clientes.columns], dados = raw_clientes.values.tolist())
inserir_dados(conexao, 'raw_compras', [f"`{col}` VARCHAR(255)" for col in raw_compras.columns], dados = raw_compras.values.tolist())
inserir_dados(conexao, 'raw_cambio', [f"`{col}` VARCHAR(255)" for col in raw_cambio.columns], dados = raw_cambio.values.tolist())



# O banco está convertendo null para 0001-01-01 00:00:00, realizando force
executar_consulta(conexao, 'UPDATE raw_compras SET data_pagamento = NULL WHERE data_pagamento ="0001-01-01 00:00:00" ')
