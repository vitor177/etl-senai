import shutil
import os

#destination_folder = r'C:\Users\joaomendonca\Documents\senai-extract\data'
network_path = "Z:\ILHA SOLTEIRA-SP_GHI_seg_ILHA_SP.dat"
#network_path = "C:\Users\joaomendonca\Documents\senai-extract\copy_files.py"
destination_folder = "./data/"


#print(os.listdir(network_path))


shutil.copy(network_path, destination_folder)