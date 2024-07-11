# %%
import pandas as pd
import os


# %%
# %%


# Função para ler um arquivo .dat e retornar um dataframe após testes de validação, com dados organizados pelo MINUTO
def etl_minute(path):

    #filename = raw\estacoes\min\ILHA SOLTEIRA-SP_Tab_dados_ILHA_SP.dat

    df_raw = pd.read_csv(path, delimiter=',', header=1)

    # Verificar se colunas conferem
    df = df_raw.iloc[2:].reset_index(drop=True)

    #df_original = df.copy()

    # %%
    # Identificando linhas duplicadas
    duplicated_rows = df[df.duplicated()]
    # %%
    df.drop_duplicates(inplace=True)
    # %%
    #df.isna().sum()
    # %%
    df["TIMESTAMP"] = pd.to_datetime(df["TIMESTAMP"])
    # Gerar série temporária
    start_timestamp = df["TIMESTAMP"].min()
    end_timestamp = df["TIMESTAMP"].max()
    all_timestamps = pd.date_range(start=start_timestamp, end=end_timestamp, freq='min')

    # %%
    # Verificar tempos faltantes
    missing_timestamps = all_timestamps.difference(df['TIMESTAMP'])
    # %%
    complete_df = pd.DataFrame(all_timestamps, columns=["TIMESTAMP"])
    merged_df = pd.merge(complete_df, df, on='TIMESTAMP', how='left')


    # %%

    return merged_df, len(duplicated_rows), len(missing_timestamps), len(merged_df)


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
