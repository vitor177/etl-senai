# %%
import pandas as pd

path = "C:/Users/joaomendonca/Documents/senai-extract/data/ILHA SOLTEIRA-SP_GHI_seg_ILHA_SP.dat"
df = pd.read_csv(path, skiprows=4, delimiter=',', header=None)
#df.columns = ["TOA5","ILHA SOLTEIRA-SP","CR1000X","23875","CR1000X.Std.04.02","CPU:ILHA SOLTEIRA_SP_Rev01.CR1X","18206","GHI_seg_ILHA_SP"]
df.columns = ["TIMESTAMP","RECORD","GHI1","GHI2","GHI3","GRI","Cell_Isc"]


df_original = df.copy()
df_original.shape
#print(df.describe())

# %%
# Identificando linhas duplicadas
duplicated_rows = df[df.duplicated()]
duplicated_rows

# %%
df.drop_duplicates(inplace=True)
df
# %%
# %%
df.isna().sum()
# %%
df["TIMESTAMP"] = pd.to_datetime(df["TIMESTAMP"])
df.head()
# %%
# Gerar série temporária
start_timestamp = df["TIMESTAMP"].min()
end_timestamp = df["TIMESTAMP"].max()
all_timestamps = pd.date_range(start=start_timestamp, end=end_timestamp, freq='S')
# %%
missing_timestamps = all_timestamps.difference(df['TIMESTAMP'])
missing_timestamps

# %%
complete_df = pd.DataFrame(all_timestamps, columns=["TIMESTAMP"])
merged_df = pd.merge(complete_df, df, on='TIMESTAMP', how='left')
# %%
merged_df.shape
# %%
