import pandas as pd 


# Transformar datas para o formato '%Y-%m-%d %H:%M:%S'

def converter_datas(dataframe):
    dataframe = dataframe.apply(pd.to_datetime, errors='coerce') # coerce substitui valores inválidos por NaT (Not a Time)
    dataframe = dataframe.dt.strftime('%Y-%m-%d %H:%M:%S')
    return dataframe