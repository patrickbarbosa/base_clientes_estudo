import pandas as pd
import sys
sys.path.append('../utils_db_ebanx')
from fn_database import conectar
from fn_database import executar_consulta
from fn_database import inserir_dados
from fn_dt import converter_datas

#Conexão
conexao = conectar('bronze')
clientes = executar_consulta(conexao, 'SELECT id, name, pais, "data nascimento", import_date FROM raw_clientes')



#   Tratamento

# Remoção de id's duplicados
clientes = clientes.drop_duplicates(subset=["id"])

# Tratando data de nascimento
clientes["data nascimento"]  = converter_datas(clientes["data nascimento"])

# Renomeando coluna
clientes = clientes.rename(columns={'data nascimento': 'data_nascimento', 'id':'id_cliente','name':'nome_cliente', 'pais':'pais_cliente'})


# Convertendo as colunas de data para o formato correto
clientes['data_nascimento'] = pd.to_datetime(clientes['data_nascimento'])
clientes['import_date'] = pd.to_datetime(clientes['import_date'])


# Alimentando tabela no silver
conexao = conectar('silver')
inserir_dados(conexao, 'stg_clientes', [f"`{col}` VARCHAR(255)" for col in clientes.columns], dados = clientes.values.tolist())