import os
import pandas as pd
from pathlib import Path
from multiprocessing import Pool, cpu_count
from copy_files import copia
from etl import etl_minute

def processar_arquivo(file_path):
    input_file = file_path.name
    print(f"Processando {input_file}")
    
    # Leitura do arquivo CSV e ajuste dos dados
    df_complete = pd.read_csv(file_path, delimiter=',', header=None, skiprows=4)
    header_lines = [line.strip() for line in open(file_path, 'r').readlines()[:4]]
    data_start_line = 4
    
    # Criação do diretório de saída
    output_base_dir = Path("output_files/min")
    output_dir = output_base_dir / input_file
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Conversão dos dados e escrita nos arquivos de saída
    for index, row in df_complete.iterrows():
        if index >= data_start_line:
            timestamp = row[0]
            print(f"TimeStamp: {timestamp} Arquivo: {file_path.name}")
            data_formatada = pd.to_datetime(timestamp).date().strftime("%Y-%m-%d")
            output_file = output_dir / f"{input_file}-min-{data_formatada}.dat"
            
            # Conversão para string e escrita no arquivo
            row_str = ','.join(map(str, row))
            
            if os.path.exists(output_file):
                with open(output_file, 'a') as file:
                    file.write(row_str + "\n")
            else:
                with open(output_file, 'w') as file:
                    for header_line in header_lines:
                        file.write(header_line + "\n")
                    file.write(row_str + "\n")

if __name__ == "__main__":

    #copia()

    seg_directory = Path("./raw/estacoes/min")
    files_to_process = list(seg_directory.glob("*"))
    
    # Paralelização com multiprocessing.Pool
    num_processes = cpu_count()  # Número de núcleos da CPU
    print(f"Utilizando {num_processes} processos.")
    
    with Pool(num_processes) as pool:
        pool.map(processar_arquivo, files_to_process)

    input_dir = Path("output_files/min")
    output_dir = Path("etl/min")

    log_dir = Path("log/min")
    log_data = {}

    # Iterar sobre os arquivos .dat no diretório de entrada
    for pasta in input_dir.glob("*"):

        if pasta.is_dir():
            station_name = pasta.name
            station_log_data = {}
            for arquivo in pasta.glob("*.dat"):
                transformed_df, qt_repetidos, qt_faltantes = etl_minute(arquivo)
                station_log_data[arquivo.stem] = {"Quantidade de repetidos": qt_repetidos, "Quantidade de faltantes": qt_faltantes}
                relative_path = arquivo.relative_to(input_dir)
                output_path = output_dir / relative_path
                output_path.parent.mkdir(parents=True, exist_ok=True)
                transformed_df.to_csv(output_path, index=False, sep=',')
            log_data[station_name] = station_log_data

    log_dir.mkdir(parents=True, exist_ok=True)
    log_file_path = log_dir / "summary_log.txt"

                
# Escrever as informações no arquivo de log geral
    with open(log_file_path, 'w') as f:
        for station, data in log_data.items():
            f.write(f"Estação: {station}\n")
            for file_name, values in data.items():
                f.write(f"  Arquivo: {file_name}\n")
                f.write(f"    Quantidade de repetidos: {values['Quantidade de repetidos']}\n")
                f.write(f"    Quantidade de faltantes: {values['Quantidade de faltantes']}\n")
            f.write("\n")