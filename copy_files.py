import shutil
import os


def copia():
    # network_drive = "D:/ESTAÇÕES ISI-ER/"
    network_drive = "/home/joaovitor/Documentos/files"

    network_path = "D:/joaovitor-ws/senai-extract"

    seg_directory = "./raw/seg"
    min_directory = "./raw/min"


    os.makedirs(seg_directory, exist_ok=True)
    os.makedirs(min_directory, exist_ok=True)

    estacoes_segundo = []
    estacoes_minuto = []


    for root, dirs, files in os.walk(network_drive):
        #print(files)
        for file in files:
            
            #if file.endswith("ATLAS-RN PAU DOS FERROS_GHI1_seg_PauFerros.dat.backup"):
            if file.endswith(".dat"):
                print(f"Copiando o arquivo {file}")
                full_path = os.path.join(network_drive, root, file)
                if "NATAL" not in file: #and "LAJES" not in file:
                    print(f"Copiando o arquivo: {file}")
                    if "seg" in file:
                        estacoes_segundo.append(full_path)
                        shutil.copy(full_path, seg_directory)
                    else:
                        estacoes_minuto.append(full_path)
                        shutil.copy(full_path, min_directory)



# if _name=="main_":
    # copia()
