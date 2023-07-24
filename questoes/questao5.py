import sys
sys.path.append('../utils_db_ebanx')
from fn_database import conectar
from fn_database import executar_consulta
import json


conexao = conectar('gold')
df = executar_consulta(conexao,"SELECT * FROM ssot_compras")


# Agrupar os dados por cliente e concatenar as categorias separadas por vírgula
result = df.groupby('nome_cliente')['categoria'].apply(lambda x: ', '.join(x.unique())).reset_index()

# Gerar o JSON a partir do DataFrame
json_data = result.to_json(orient='records', force_ascii=False) # force_ascii para permitir acentos

json_string = json.dumps(json_data, ensure_ascii=False)

# Salvar a string do JSON em um arquivo
with open('questao5.json', 'w',encoding='utf-8') as file:
    file.write(json_string)
    print('Arquivo JSON gerado')