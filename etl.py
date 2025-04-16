import pandas as pd
import numpy as np
from datetime import datetime

def calcular_dia_juliano(timestamp_str):
    timestamp = datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S')
    return timestamp.timetuple().tm_yday

def timestamp_para_horalocal(timestamp_str):
    timestamp = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S")
    horalocal = timestamp.hour * 60 + timestamp.minute
    return horalocal

def etl_minute(path):

    header_lines = [line.strip() for line in open(path, 'r').readlines()[:4]]

    #print("HEADER LINES: ", header_lines)

    df_raw = pd.read_csv(path, delimiter=',', header=1)
    df = df_raw.iloc[2:].reset_index(drop=True)
    
    duplicated_rows = df[df.duplicated()]
    df.drop_duplicates(inplace=True)
    df["TIMESTAMP"] = pd.to_datetime(df["TIMESTAMP"])
    
    #start_timestamp = df["TIMESTAMP"].min()
    start_timestamp = df["TIMESTAMP"].min().normalize()
    #end_timestamp = df["TIMESTAMP"].max()
    end_timestamp = (df["TIMESTAMP"].max().normalize() + pd.offsets.Day()) - pd.Timedelta(minutes=1)

    all_timestamps = pd.date_range(start=start_timestamp, end=end_timestamp, freq='min')
    
    missing_timestamps = all_timestamps.difference(df['TIMESTAMP'])
    complete_df = pd.DataFrame(all_timestamps, columns=["TIMESTAMP"])


    merged_df = pd.merge(complete_df, df, on='TIMESTAMP', how='left')
    
    latitude = -5.706841
    longitude = -36.232853

     # RNES03
    if "RNES03" in str(path):
        latitude = -6.1440
        longitude = -38.1904
    # RNES04
    if "RNES04" in str(path):
        latitude = -6.2287
        longitude = -36.0276
    # RNES02
    if "RNES02" in str(path):
        latitude = -5.2962
        longitude = -36.2728
    # RNES01
    if "RNES01" in str(path):
        latitude = -5.7068
        longitude = -36.2300
    # SPES01
    if "SPES01" in str(path):
        print("COMEÇANDO ETL MINUTE EM : ", path)
        latitude = -21.9807
        longitude = -47.4525
    # PBES01
    if "PBES01" in str(path):
        latitude = -6.8372
        longitude = -38.2934
    if "ILHA SOLTEIRA" in str(path):
        pass

    longitude_ref = -45
    isc = 1367
    
    colunas = df.columns
    ghi_avg_colunas = [coluna for coluna in colunas if 'GHI' in coluna and coluna.endswith('_Avg')]
    info_ghi = {col: {'contador_fisicamente_possivel': 0} for col in ghi_avg_colunas}

    for index, row in merged_df.iterrows():
        timestamp_str = str(row['TIMESTAMP'])
        dia_juliano = calcular_dia_juliano(timestamp_str)
        
        rad = (2*np.pi*(dia_juliano-1)/365)
        eo = 1.00011 + (0.0334221 * np.cos(rad)) + (0.00128 * np.sin(rad)) + (0.000719 * np.cos(2 * rad)) + (0.000077 * np.sin(2 * rad))
        io = isc*eo
        iox = max(io, 0)
        
        horalocal = timestamp_para_horalocal(timestamp_str)
        et = 229.18 * (0.000075 + 0.001868 * np.cos(rad) - 0.032077 * np.sin(rad) - 0.014615 * np.cos(2*rad) - 0.04089 * np.sin(2 * rad))
        horasolar = (horalocal + ((4 * (longitude - longitude_ref)) + et)) / 60
        omega = (horasolar - 12) * 15
        declinacao = 23.45 * np.sin(((dia_juliano + 284) * (360 / 365)) * np.pi / 180)
        cosAZS = (np.sin(latitude * np.pi / 180) * np.sin(declinacao * np.pi / 180)) + (np.cos(latitude * np.pi / 180) *
                 np.cos(declinacao * np.pi / 180) * np.cos(omega * np.pi / 180))
        AZS = np.arccos(cosAZS) * 180 / np.pi
        
        cosAZS12 = cosAZS ** 1.2 if AZS <= 90 else 0
        fpmin = -4
        fpmax = (1.5 * iox * cosAZS12) + 100
        
        for col in ghi_avg_colunas:
            value = row[col]
            if pd.notna(value) and isinstance(value, (int, float, str)):
                try:
                    value = float(value)
                    if fpmin < value < fpmax:
                        info_ghi[col]['contador_fisicamente_possivel'] += 1
                except ValueError:
                    continue
    
    return merged_df, len(duplicated_rows), len(missing_timestamps), len(merged_df), info_ghi, header_lines


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
    # %%
    df["TIMESTAMP"] = pd.to_datetime(df["TIMESTAMP"])
    # Gerar série temporária
    
    #start_timestamp = df["TIMESTAMP"].min()
    start_timestamp = df["TIMESTAMP"].min().normalize()

    #end_timestamp = df["TIMESTAMP"].max()
    end_timestamp = (df["TIMESTAMP"].max().normalize() + pd.offsets.Day()) - pd.Timedelta(seconds=1)

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
