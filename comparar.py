import pandas as pd

# Caminho dos arquivos
arquivo_entrada = 'consolidado_PBES01.xlsx'
arquivo_saida = 'consolidado_PBES01_sem_duplicatas.xlsx'

# Lê os dados a partir da 5ª linha (dados principais)
df = pd.read_excel(arquivo_entrada, skiprows=4)

# Verifica se há colunas duplicadas (isso cria os .1, .2...)
colunas_duplicadas = df.columns[df.columns.duplicated()].tolist()
if colunas_duplicadas:
    print(f"⚠️ Atenção: Há colunas duplicadas, como: {colunas_duplicadas}")

# Trabalha com a primeira coluna (sem renomear)
coluna_timestamp = pd.to_datetime(df.iloc[:, 0], dayfirst=True, errors='coerce')

# Remove duplicatas com base nessa coluna
df_sem_duplicatas = df[~coluna_timestamp.duplicated(keep='first')].copy()

# Lê o cabeçalho original (linhas 1 a 4)
df_header = pd.read_excel(arquivo_entrada, nrows=4, header=None)

# Salva em novo arquivo
with pd.ExcelWriter(arquivo_saida, engine='openpyxl') as writer:
    df_header.to_excel(writer, index=False, header=False)
    df_sem_duplicatas.to_excel(writer, index=False, startrow=4)

# Exibe resumo
print(f"✅ Linhas originais: {len(df)}")
print(f"✅ Linhas após remoção de duplicatas: {len(df_sem_duplicatas)}")
print(f"💾 Arquivo salvo como: {arquivo_saida}")
