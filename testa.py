import pandas as pd

# Exemplo de string de data
data_string = "2024-11-30 00:00:00"

# Converter para datetime
data_datetime = pd.to_datetime(data_string, format='%Y-%m-%d %H:%M:%S')

# Alterar o formato para DD/MM/YYYY HH:MM:SS
data_formatada = data_datetime.strftime('%d/%m/%Y %H:%M:%S')

# Exibir o resultado
print(data_formatada)
