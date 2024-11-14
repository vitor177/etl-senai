import pandas as pd
from datetime import datetime, timedelta
import os
import time
# Obtém o ID do processo atual
process_id = os.getpid()

print(f"Número do processo: {process_id}")
# input()#Pausar o process
time_star = time.perf_counter_ns()

file_path = "/home/cemig/Documentos/GitHub/ESTAÇÕES ISI-ER/RNES00 - NATAL/ES_ISI_ER NATAL_Dados_seg_Natal.dat"

# Define o período de interesse
one_month_ago = datetime.now() - timedelta(days=15)
print("Pegar valores desde: ")
print(one_month_ago)
# Inicializa uma lista para armazenar os chunks filtrados
filtered_data = []

# Lê o arquivo em chunks e filtra as linhas conforme necessário
for chunk in pd.read_csv(file_path, chunksize=100000,  parse_dates=[0], header=None,  skiprows=4):
    # chunk[0] = pd.to_datetime(chunk[0], format='%Y-%m-%d %H:%M:%S')
    filtered_chunk = chunk[(chunk[0] > one_month_ago)]
    # filtered_chunk = chunk[(chunk[0] == 02-11-2024 ) || (chunk[0] == '03-11-2024' )]
    filtered_data.append(filtered_chunk)

# Concatena os chunks filtrados
df = pd.concat(filtered_data, ignore_index=True)

print(df)

time_end = time.perf_counter_ns()


print("Tempo total de execução: ")
print(((time_end - time_star)/1_000_000_000), " seconds")


#Limita a RAm em 8.71
