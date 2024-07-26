from datetime import datetime
from datetime import date
from datetime import timedelta
uma_semana_atras = date.today() - timedelta(days=2)

print(uma_semana_atras)


print(datetime.now().date())


# 1 - Alterar ordem do arquivo de LOG

# 2 - Ao iniciar o script, excluir os diretórios: raw, bronze, silver, gold e log se existirem

# 3 - Agendar o script para executar 5 da manhã todos os dias

# 4 - Excluir o dia atual 