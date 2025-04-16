import sys
import os
import pandas as pd
from pathlib import Path
from multiprocessing import Pool, cpu_count
from copy_files import copia
from etl import etl_minute
import shutil
from merge_dat_files import merge_dat_files

from datetime import datetime, timedelta, date
import time

#Se as datas nao forem enseridas manualmente
if len(sys.argv) < 2:
    # Verifica se o dia de hoje é segunda-feira
    if datetime.now().weekday() == 0:#Dados de Sexta, Sabado e Domingo
        initial_data = (pd.Timestamp('today') - pd.Timedelta(days=4)).replace(hour=23, minute=59, second=59)
        final_data = (pd.Timestamp('today') - pd.Timedelta(days=1)).replace(hour=23, minute=59, second=59)
    else:#Dados do dia Anterior
        initial_data = (pd.Timestamp('today') - pd.Timedelta(days=2)).replace(hour=23, minute=59, second=59)#
        final_data = (pd.Timestamp('today') - pd.Timedelta(days=1)).replace(hour=23, minute=59, second=59)
else:
    # search_data_start = "2024-10-04"#None# "yyyy-mm-dd" or None, example "2024-10-13"
    # search_data_end = "2024-10-05"#None# "yyyy-mm-dd" or None, example "2024-10-13"
    search_data_start = sys.argv[1]
    search_data_end = sys.argv[2]
    #Conversão para as 23:59:59 do dia anterior
    initial_data = (pd.to_datetime(search_data_start+' 23:59:59') - pd.Timedelta(days=1)).replace(hour=23, minute=59, second=59)
    final_data = pd.to_datetime(search_data_end+' 23:59:59')



def processar_arquivo_segundo(file_path):
    original_name = file_path.name
    input_file = file_path.name

    print(f"Processando {input_file}")

     # RNES03
    if "PAU DOS FERROS" in str(input_file):
        input_file = "RNES03"
    # RNES04
    if "SANTA CRUZ" in str(input_file):
        input_file = "RNES04"
    # RNES02
    if "JANDAIRA" in str(input_file):
        input_file = "RNES02"
    # RNES01
    if "LAJES" in str(input_file):
        input_file = "RNES01"
    # SPES01
    if "PIRASSUNUNGA" in str(input_file):
        input_file = "SPES01"
    # PBES01
    if "SOUSA" in str(input_file):
        input_file = "PBES01"
    # Falta Ilha Solteira
    if "ILHA SOLTEIRA" in str(input_file):
        input_file = "SPES02"
    if "NATAL" in str(input_file):
        input_file = "RES00"
    if "NOVA CRUZ" in str(input_file):
        input_file = "RNES05"
    if "MOSSORO" in str(input_file):
        input_file = "RNES06"
    
    # Leitura do arquivo CSV e ajuste dos dados
    df_complete = pd.read_csv(file_path, delimiter=',', header=None, skiprows=4)
    df_complete[0] = pd.to_datetime(df_complete[0], format='%Y-%m-%d %H:%M:%S')

    header_lines = [line.strip() for line in open(file_path, 'r').readlines()[:4]]
    data_start_line = 4
    
    # Criação do diretório de saída
    output_base_dir = Path("bronze/seg")
    output_dir = output_base_dir / input_file
    output_dir.mkdir(parents=True, exist_ok=True)

    df_outdated_data = df_complete[(df_complete[0] > initial_data) & (df_complete[0] <= final_data)]




    # Conversão dos dados e escrita nos arquivos de saída
    for index, row in df_outdated_data.iterrows():
        #if index >= data_start_line:
        timestamp = row[0]
        data_timestamp = pd.to_datetime(timestamp).date()
        if not pd.isna(data_timestamp):
            #if data_timestamp > uma_semana_atras and data_timestamp != datetime.now().date():
            #print(f"TimeStamp: {timestamp} Arquivo: {file_path.name}")
            data_formatada = pd.to_datetime(timestamp).date().strftime("%Y-%m-%d")
            output_file = output_dir / f"{input_file}_{data_formatada}.dat"

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


def processar_arquivo_minuto(file_path):
    input_file = file_path.name
    print(f"Processando {input_file}")

     # RNES03
    if "PAU DOS FERROS" in str(input_file):
        input_file = "RNES03"
    # RNES04
    if "SANTA CRUZ" in str(input_file):
        input_file = "RNES04"
    # RNES02
    if "JANDAIRA" in str(input_file):
        input_file = "RNES02"
    # RNES01
    if "LAJES" in str(input_file):
        input_file = "RNES01"
    # SPES01
    if "PIRASSUNUNGA" in str(input_file):
        input_file = "SPES01"
    # PBES01
    if "SOUSA" in str(input_file):
        input_file = "PBES01"
    # Falta Ilha Solteira
    if "ILHA SOLTEIRA" in str(input_file):
        input_file = "SPES02"
    if "NATAL" in str(input_file):
        input_file = "RES00"
    if "NOVA CRUZ" in str(input_file):
        input_file = "RNES05"
    if "MOSSORO" in str(input_file):
        input_file = "RNES06"
    
    # Leitura do arquivo CSV e ajuste dos dados
    df_complete = pd.read_csv(file_path, delimiter=',', header=None, skiprows=4)
    df_complete[0] = pd.to_datetime(df_complete[0], format='%Y-%m-%d %H:%M:%S')
    header_lines = [line.strip() for line in open(file_path, 'r').readlines()[:4]]
    data_start_line = 4
    
    # Criação do diretório de saída
    output_base_dir = Path("bronze/min")
    output_dir = output_base_dir / input_file
    output_dir.mkdir(parents=True, exist_ok=True)
    

    df_outdated_data = df_complete[(df_complete[0] > initial_data) & (df_complete[0] <= final_data)]

    
    # Conversão dos dados e escrita nos arquivos de saída
    for index, row in df_outdated_data.iterrows():
        # if index >= data_start_line:
        timestamp = row[0]

        data_timestamp = pd.to_datetime(timestamp).date()

        if not pd.isna(data_timestamp):
            # if data_timestamp > uma_semana_atras and data_timestamp != datetime.now().date():
            #if data_timestamp >= datetime.strptime('2024-07-31', '%Y-%m-%d').date() and data_timestamp <= datetime.strptime('2024-08-31', '%Y-%m-%d').date():
            #print(f"TimeStamp: {timestamp} Arquivo: {file_path.name}")
            data_formatada = pd.to_datetime(timestamp).date().strftime("%Y-%m-%d")
            output_file = output_dir / f"{input_file}_{data_formatada}.dat"
            
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

    # Exclusão das pastas ao executar o script
    directories = ["raw", "bronze", "silver", "gold", "log", "consolidado"]

    for directory in directories:
        if os.path.exists(directory):
            shutil.rmtree(directory)

    # COPIA DOS ARQUIVOS PARA A PASTA RAW
    copia() 

    min_directory = Path("./raw/min")
    files_to_process_min = list(min_directory.glob("*"))
    
    seg_directory = Path("./raw/seg")
    files_to_process_seg = list(seg_directory.glob("*"))

    # Paralelização com multiprocessing.Pool
    num_processes = cpu_count()  # Número de núcleos da CPU
    print(f"Utilizando {num_processes} processos.")
    
    
    with Pool(num_processes) as pool:
        pool.map(processar_arquivo_minuto, files_to_process_min)
    
    with Pool(num_processes) as pool:
        pool.map(processar_arquivo_segundo, files_to_process_seg)
    
    input_dir = Path("bronze/min")
    output_dir = Path("silver/min")

    log_dir = Path("log/min")
    log_data = {}

    # CRIAÇÃO DO ARQUIVO DE LOG
    for pasta in input_dir.glob("*"):
        if pasta.is_dir():
            station_name = pasta.name
            station_log_data = {}
            for arquivo in pasta.glob("*.dat"):
                print("PROCESSANDO ARQUIVO: ", arquivo)
                transformed_df, qt_repetidos, qt_faltantes, qt_amostras, contador_fisicamente_possivel, header_lines = etl_minute(arquivo)
                station_log_data[arquivo.stem] = {"Quantidade de repetidos": qt_repetidos, "Quantidade de faltantes": qt_faltantes, "Quantidade de amostras": qt_amostras, "Contador Fisicamente Possível": contador_fisicamente_possivel}
                relative_path = arquivo.relative_to(input_dir)
                output_path = output_dir / relative_path
                output_path.parent.mkdir(parents=True, exist_ok=True)
                transformed_df = transformed_df.applymap(lambda x: 'NAN' if pd.isna(x) else x)

                with open(output_path, 'w') as f:
                    for line in header_lines:
                        print(line)
                        f.write(line + '\n')
                    transformed_df.to_csv(f, index=False, header=False,sep=',')
            log_data[station_name] = station_log_data

    log_dir.mkdir(parents=True, exist_ok=True)
    log_file_path = log_dir / "summary_log.txt"

                
    # ESCRITA DO ARQUIVO DE LOG
    with open(log_file_path, 'w') as f:
        for station, data in log_data.items():
            f.write(f"Estação: {station}\n")
            for file_name, values in data.items():
                f.write(f"    Quantidade de amostras: {values['Quantidade de amostras']}\n")
                f.write(f"  Arquivo: {file_name}\n")
                f.write(f"    Quantidade de repetidos: {values['Quantidade de repetidos']}\n")
                f.write(f"    Quantidade de faltantes: {values['Quantidade de faltantes']}\n")
                f.write(f"    Quantidade de amostras fisicamente possíveis:\n")
                for key, val in values['Contador Fisicamente Possível'].items():
                    f.write(f"      {key}: {val['contador_fisicamente_possivel']}\n")
                    faltantes = values['Quantidade de faltantes']
                    total = values['Quantidade de amostras']
                    n = val['contador_fisicamente_possivel']
                    anomalos = total - faltantes - n
                    #status = ""
                    if anomalos/(total-faltantes) < 0.01:
                        status = "Consistente"
                    elif anomalos/(total-faltantes) >= 0.01 and anomalos/(total-faltantes) <= 0.05:
                        status = "Atenção"
                    else:
                        status = "Inconsistente"

                    porcentagem = (anomalos/(total-faltantes))*100

                    f.write(f"          Dados Anômalos: {porcentagem:,.2f}%\n")
                    f.write(f"          Situação: {status}\n")
            f.write("\n")

    # MERGE DOS ARQUIVOS
    input_dir = Path("silver/min")

    for pasta in input_dir.glob("*"):
        novo_arquivo = pasta.stem

        final_dir = Path(f"gold/min/{novo_arquivo}")
        final_file = final_dir / f"{novo_arquivo}.dat"

        final_dir.mkdir(parents=True, exist_ok=True)
        merge_dat_files(pasta, final_file)


    # print("Buscas a partir de: ",initial_data )
    # print("Até: ", final_data)

    time_end = time.perf_counter_ns()
