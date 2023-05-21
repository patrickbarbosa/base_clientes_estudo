import pandas as pd

import sys
sys.path.append('../utils_db_ebanx')
from fn_database import conectar
from fn_database import executar_consulta
from fn_database import inserir_dados

#Conexão
conexao = conectar('bronze')

# Adicionando colunas auxiliares
compras = executar_consulta(conexao,'''

    SELECT 
        A.*
        ,CASE WHEN status = "Pago" AND data_vencimento < data_pagamento THEN 1 ELSE 0 END AS flg_pago_vencido
        ,CASE WHEN DATEDIFF(data_pagamento,data_vencimento) > 0 THEN (DATEDIFF(data_pagamento,data_vencimento)) ELSE 0 END AS dias_atraso
        ,MONTH(data_pagamento) AS mes_pagamento
    FROM raw_compras A
''')

#Tratando casos em que não existe data de pagamento e a consulta retorna NaN para a operação MONTH. Trocando NaN por 0
compras['mes_pagamento'] = compras['mes_pagamento'].fillna(0).astype(int)

# Renomeando coluna
compras = compras.rename(columns={'id': 'id_venda'})

# Inserindo dados em tabela Silver

conexao = conectar('silver')
inserir_dados(conexao, 'stg_compras', [f"`{col}` VARCHAR(255)" for col in compras.columns], dados = compras.values.tolist())