import shutil
import os


def copia():
    network_drive = "Z:/"
    #network_path = "C:\Users\joaomendonca\Documents\senai-extract\copy_files.py"

    seg_directory = "./raw/seg"
    min_directory = "./raw/min"


    os.makedirs(seg_directory, exist_ok=True)
    os.makedirs(min_directory, exist_ok=True)

    estacoes_segundo = []
    estacoes_minuto = []

    for filename in os.listdir(network_drive):
        if filename.endswith(".dat"):
            print(f"Copiando o arquivo {filename}")
            full_path = os.path.join(network_drive, filename)
            if "seg" in filename:
                estacoes_segundo.append(full_path)
                shutil.copy(full_path, seg_directory)
            else:
                estacoes_minuto.append(full_path)
                shutil.copy(full_path, min_directory)

    assert len(estacoes_minuto) == len(estacoes_segundo)

    print("Arquivos copiados com sucesso!")


