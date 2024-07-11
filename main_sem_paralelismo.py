import os
import pandas as pd
from pathlib import Path

if __name__ == "__main__":
    seg_directory = Path("./raw/estacoes/seg")
    output_base_dir = Path("output_files/seg")
    
    for file_path in seg_directory.glob("*"):
        input_file = file_path.name
        print(f"Processando {input_file}")
        
        # Leitura do arquivo CSV e ajuste dos dados
        df_complete = pd.read_csv(file_path, delimiter=',', header=None, skiprows=4)
        header_lines = [line.strip() for line in open(file_path, 'r').readlines()[:4]]
        data_start_line = 4
        
        # Criação do diretório de saída
        output_dir = output_base_dir / input_file
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Conversão dos dados e escrita nos arquivos de saída
        for index, row in df_complete.iterrows():
            if index >= data_start_line:
                timestamp = row[0]
                data_formatada = pd.to_datetime(timestamp).date().strftime("%Y-%m-%d")
                output_file = output_dir / f"{input_file}-seg-{data_formatada}.dat"
                
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
