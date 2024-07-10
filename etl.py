# %%
import pandas as pd

# Função para ler um arquivo .dat e retornar um dataframe após testes de validação, com dados organizados pelo MINUTO
def etl_minute(filename):

    #filename = raw\estacoes\min\ILHA SOLTEIRA-SP_Tab_dados_ILHA_SP.dat

    df = pd.read_csv(f"raw/estacoes/min/{filename}", skiprows=4, delimiter=',', header=None)

    # Verificar se colunas conferem
    df.columns = ["TIMESTAMP","RECORD","ID","Ano","Dia","MinDia","T_Bat","TEMP_Avg","TEMP_Max","TEMP_Min","TEMP_Std","UR_Avg","UR_Max","UR_Min","UR_Std","PRES_Avg","PRES_Max","PRES_Min","PRES_Std","Prec_Tot","VEL_Avg","VEL_Max","VEL_Min","VEL_Std","DIR_Avg","DIR_Max","DIR_Min","DIR_Std","GHI1_Avg","GHI1_Max","GHI1_Min","GHI1_Std","DHI_Avg","DHI_Max","DHI_Min","DHI_Std","BNI_Avg","BNI_Max","BNI_Min","BNI_Std","Temp_BNI","Res_BNI","LWD_Avg","LWD_Std","LWD_Max","LWD_Min","OLD_Avg","OLD_Std","OLD_Max","OLD_Min","Temp_OLD","Res_OLD"]

    #df_original = df.copy()

    # %%
    # Identificando linhas duplicadas
    #duplicated_rows = df[df.duplicated()]
    # %%
    df.drop_duplicates(inplace=True)
    # %%
    #df.isna().sum()
    # %%
    df["TIMESTAMP"] = pd.to_datetime(df["TIMESTAMP"])
    # Gerar série temporária
    start_timestamp = df["TIMESTAMP"].min()
    end_timestamp = df["TIMESTAMP"].max()
    all_timestamps = pd.date_range(start=start_timestamp, end=end_timestamp, freq='T')

    # %%
    # Verificar tempos faltantes
    missing_timestamps = all_timestamps.difference(df['TIMESTAMP'])
    # %%
    complete_df = pd.DataFrame(all_timestamps, columns=["TIMESTAMP"])
    merged_df = pd.merge(complete_df, df, on='TIMESTAMP', how='left')
    # %%

    return merged_df


# %%
# Função para ler um arquivo .dat e retornar um dataframe após testes de validação, com dados organizados pelo SEGUNDO
def etl_second(filename):

    df = pd.read_csv(f"raw/estacoes/sec/{filename}", skiprows=4, delimiter=',', header=None)

    # Verificar como automatizar isso :D
    df.columns = ["TIMESTAMP","RECORD","GHI1","GHI2","GHI3","GRI","Cell_Isc"]

    df_original = df.copy()
    df_original.shape
    # %%
    # Identificando linhas duplicadas
    #duplicated_rows = df[df.duplicated()]

    # %%
    df.drop_duplicates(inplace=True)
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
    return merged_df
    # %%
