# %%
import pandas as pd
import os
import csv
from datetime import datetime

path = "C:/Users/joaomendonca/Documents/senai-extract/data/ILHA SOLTEIRA-SP_GHI_seg_ILHA_SP.dat"
input_file = path.split("/")[-1]
df_complete = pd.read_csv(path, delimiter=',', header=None, skiprows=4)

df = df_complete.iloc[4:].reset_index(drop=True)
#df.columns = ["TOA5","ILHA SOLTEIRA-SP","CR1000X","23875","CR1000X.Std.04.02","CPU:ILHA SOLTEIRA_SP_Rev01.CR1X","18206","GHI_seg_ILHA_SP"]
#df.columns = ["TIMESTAMP","RECORD","GHI1","GHI2","GHI3","GRI","Cell_Isc"]


df_original = df.copy()
df_original.shape
# %%

# %%
df_complete.head()
# %%
header_lines = []
data_start_line = None

with open(path, 'r') as file:
    for i, line in enumerate(file):
        if i < 4:
            header_lines.append(line.strip())
        else:
            data_start_line = i
            break
# %%
header_lines



# %%

# Create a directory to store the output files
output_dir = f"output_files/{input_file}"
os.makedirs(output_dir, exist_ok=True)

# %%
df.head()
# %%


for index, row in df_complete.iterrows():
    if index >= data_start_line:
        #print(row)
        timestamp = row[0]
        print(timestamp)
        #break

        data = pd.to_datetime(timestamp).date()
        data_formatada = data.strftime("%Y-%m-%d")


          # Nome do arquivo de saída
        output_file = os.path.join(output_dir, f"{input_file}-SEG-{data_formatada}.dat")

        row = [str(item) for item in row]

          # Verificar se o arquivo já existe
        if os.path.exists(output_file):
        # Adicionar ao final do arquivo existente
            with open(output_file, 'a') as file:
                file.write(",".join(row) + "\n")
        else:
        # Criar um novo arquivo e salvar os dados da medição
            with open(output_file, 'w') as file:
                            # Escrever cada linha do cabeçalho separadamente
                for header_line in header_lines:
                    file.write(header_line + "\n")
                file.write(",".join(row) + "\n")  # Escreve os dados da medição
