# %%
import pandas as pd

df = pd.read_csv('C:/Users/joaomendonca/Documents/senai-extract/data/PIRASSUNUNGA-SP_Tabela_ES02.dat', skiprows=4, delimiter=',', header=None)
df.columns = ["TIMESTAMP","RECORD","ID","Ano","Dia","MinDia","T_Bat","TEMP_Avg","TEMP_Max","TEMP_Min","TEMP_Std","UR_Avg","UR_Max","UR_Min","UR_Std","PRES_Avg","PRES_Max","PRES_Min","PRES_Std","Prec_Tot","VEL_Avg","VEL_Max","VEL_Min","VEL_Std","DIR_Avg","DIR_Max","DIR_Min","DIR_Std","GHI1_Avg","GHI1_Max","GHI1_Min","GHI1_Std","DHI_Avg","DHI_Max","DHI_Min","DHI_Std","BNI_Avg","BNI_Max","BNI_Min","BNI_Std","Temp_BNI","Res_BNI","LWD_Avg","LWD_Std","LWD_Max","LWD_Min","OLD_Avg","OLD_Std","OLD_Max","OLD_Min","Temp_OLD","Res_OLD"]

df_original = df.copy()
df_original.shape
#print(df.describe())
# %%
df.shape

# %%
# Identificando linhas duplicadas
duplicated_rows = df[df.duplicated()]
duplicated_rows

# %%
df.drop_duplicates(inplace=True)
df
# %%
df.isna().sum()

# %%
df.head()
df["TIMESTAMP"] = pd.to_datetime(df["TIMESTAMP"])

# Gerar série temporária
start_timestamp = df["TIMESTAMP"].min()
end_timestamp = df["TIMESTAMP"].max()
all_timestamps = pd.date_range(start=start_timestamp, end=end_timestamp, freq='T')

# %%
# Verificar tempos faltantes
missing_timestamps = all_timestamps.difference(df['TIMESTAMP'])
missing_timestamps

# %%
complete_df = pd.DataFrame(all_timestamps, columns=["TIMESTAMP"])
merged_df = pd.merge(complete_df, df, on='TIMESTAMP', how='left')
# %%
