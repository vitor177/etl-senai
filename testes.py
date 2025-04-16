# %%

import pandas as pd

df = pd.read_csv('C:/Users/joaomendonca/Documents/senai-extract/data/PIRASSUNUNGA-SP_Tabela_ES02.dat', skiprows=4, delimiter=',', header=None)
df.columns = ["TIMESTAMP","RECORD","ID","Ano","Dia","MinDia","T_Bat","TEMP_Avg","TEMP_Max","TEMP_Min","TEMP_Std","UR_Avg","UR_Max","UR_Min","UR_Std","PRES_Avg","PRES_Max","PRES_Min","PRES_Std","Prec_Tot","VEL_Avg","VEL_Max","VEL_Min","VEL_Std","DIR_Avg","DIR_Max","DIR_Min","DIR_Std","GHI1_Avg","GHI1_Max","GHI1_Min","GHI1_Std","DHI_Avg","DHI_Max","DHI_Min","DHI_Std","BNI_Avg","BNI_Max","BNI_Min","BNI_Std","Temp_BNI","Res_BNI","LWD_Avg","LWD_Std","LWD_Max","LWD_Min","OLD_Avg","OLD_Std","OLD_Max","OLD_Min","Temp_OLD","Res_OLD"]

df_original = df.copy()
df_original.shape
#print(df.describe())


# %%
import os
input_file = "PIRASSUNUNGA-SP_Tabela_ES02.dat"
base_file_name = os.path.splitext(input_file)[0]
base_file_name
# %%
header_lines = []
data_start_line = None
with open("C:/Users/joaomendonca/Documents/senai-extract/data/PIRASSUNUNGA-SP_Tabela_ES02.dat", 'r') as file:
    for i, line in enumerate(file):
        if i < 4:
            header_lines.append(line.strip())
        else:
            data_start_line = i
            break

# %%
header_lines
# %%
data_start_line
# %%
# Create a directory to store the output files
output_dir = "bronze"
os.makedirs(output_dir, exist_ok=True)

# Read the data and separate by date
current_date = None
current_date_data = []

# %%
import csv
with open("C:/Users/joaomendonca/Documents/senai-extract/data/PIRASSUNUNGA-SP_Tabela_ES02.dat", 'r') as file:
    reader = csv.reader(file)
    for i, row in enumerate(reader):
        if i >= data_start_line:
            timestamp = row[0]
            date = timestamp.split()[0]
            print("TimeSTamp: ", timestamp)
            print(f"Date: {date}")
            if date != current_date:
                # Save previous day's data if there's any
                if current_date:
                    output_file = os.path.join(output_dir, f"{base_file_name}_{current_date}.dat")
                    with open(output_file, 'w', newline='') as output:
                        ...
                        #output.writelines('\n'.join(header_lines) + '\n')
                        #output.writelines('\n'.join(current_date_data) + '\n')

                 # Reset current date data
            current_date = date
            current_date_data = []

            # Check and add quotes around NAN values
            for j in range(len(row)):
                if row[j] == "NAN":
                    row[j] = '"NAN"'

            # Check and add quotes around INF values
            for j in range(len(row)):
                if row[j] == "INF":
                    row[j] = '"INF"'

            # Add quotes around the timestamp
            row[0] = f'"{timestamp}"'
            current_date_data.append(','.join(row))

            print(f"ROW 0 TA RECEBENDO {timestamp}")
            print(f"CURRENT_DATE_DATA É {len(current_date_data)}")

            break


                

timestamp
# %%
date
# %%
# Save the last day's data
if current_date and current_date_data:
    output_file = os.path.join(output_dir, f"{base_file_name}_{current_date}.dat")
    with open(output_file, 'w', newline='') as output:
        output.writelines('\n'.join(header_lines) + '\n')
        output.writelines('\n'.join(current_date_data) + '\n')

print("Separation and saving completed.")

# %%