import pandas as pd
import sys
sys.path.append('../utils_db_ebanx')
from fn_database import conectar
from fn_database import executar_consulta
from fn_database import inserir_dados


#Conexão
conexao = conectar('bronze')
raw_cambio = executar_consulta(conexao,"SELECT Mês,BRL,EUR,CNY,EGP,KRW,CLP,MXN, import_date FROM raw_cambio")


#Renomeando coluna
raw_cambio = raw_cambio.rename(columns={'Mês': 'nome_mes'})

# Criação do num_mes para linkar com a raw_cambio de compras e realizar cálculo de multa
for index, row in raw_cambio.iterrows():

    if raw_cambio.at[index, 'nome_mes'] == "Janeiro":
        raw_cambio.at[index, 'num_mes'] = '1'
    elif raw_cambio.at[index, 'nome_mes'] == "Fevereiro":
        raw_cambio.at[index, 'num_mes'] = '2'
    elif raw_cambio.at[index, 'nome_mes'] == "Março":
        raw_cambio.at[index, 'num_mes'] = '3'
    elif raw_cambio.at[index, 'nome_mes'] == "Abril":
        raw_cambio.at[index, 'num_mes'] = '4'
    elif raw_cambio.at[index, 'nome_mes'] == "Maio":
        raw_cambio.at[index, 'num_mes'] = '5'
    elif raw_cambio.at[index, 'nome_mes'] == "Junho":
        raw_cambio.at[index, 'num_mes'] = '6'
    elif raw_cambio.at[index, 'nome_mes'] == "Julho":
        raw_cambio.at[index, 'num_mes'] = '7'
    elif raw_cambio.at[index, 'nome_mes'] == "Agosto":
        raw_cambio.at[index, 'num_mes'] = '8'
    elif raw_cambio.at[index, 'nome_mes'] == "Setembro":
        raw_cambio.at[index, 'num_mes'] = '9'
    elif raw_cambio.at[index, 'nome_mes'] == "Outubro":
        raw_cambio.at[index, 'num_mes'] = '10'
    elif raw_cambio.at[index, 'nome_mes'] == "Novembro":
        raw_cambio.at[index, 'num_mes'] = '11'
    elif raw_cambio.at[index, 'nome_mes'] == "Dezembro":
        raw_cambio.at[index, 'num_mes'] = '12'


# Utilizando melt para despivotar a tabela e assim conseguir conectar com a tabela de compras pelo idMes e a Moeda
raw_cambio = pd.melt(raw_cambio, id_vars=["nome_mes","num_mes","import_date"])
raw_cambio = raw_cambio.rename(columns={'variable': 'moeda_cambio', 'value': 'valor_cambio'})


#Inserindo Dados no Silver
conexao = conectar('silver')
inserir_dados(conexao, "stg_cambio", [f"`{col}` VARCHAR(255)" for col in raw_cambio.columns], dados = raw_cambio.values.tolist())
