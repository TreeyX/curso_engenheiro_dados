# %%
import pandas as pd
import yaml
import os
from pathlib import Path
from sqlalchemy import create_engine, text, inspect

# %%
with open('./config.yaml', 'r', encoding='utf-8') as arquivo_configuracao:
    config = yaml.safe_load(arquivo_configuracao)

postgres_config = config['credenciais_postgres']
caminho_final = config['caminho_etl_final']

# %%
connection_string = f"postgresql+psycopg2://{postgres_config['user']}:{postgres_config['password']}@{postgres_config['host']}:{postgres_config['port']}/{postgres_config['database']}"
engine = create_engine(connection_string)
raw_engine = engine.raw_connection()
insp = inspect(engine)

# %%
with open('./ddl/create_table.sql', 'r', encoding='utf8') as criar_tabela:
    query = criar_tabela.read()

with engine.begin() as conn:
    conn.execute(text(query))

# %%
caminho_arquivos_raw = Path(fr".\data\raw_data")
lista_arquivos_raw = list(caminho_arquivos_raw.glob("*.csv"))


# %%
for arquivos in lista_arquivos_raw:
    arquivo = os.path.basename(arquivos)
    nome_tabela = arquivo.split(".csv")[0]
    with engine.begin() as conn:
        resultado = conn.execute(text(f"select 1 from {nome_tabela} limit 1")).fetchone()

        if resultado is None:
            df = pd.read_csv(fr".\{caminho_arquivos_raw}\{nome_tabela}.csv", sep=';', encoding='utf-8')
            df.to_sql(nome_tabela, conn, if_exists='append', index=False)
        else:
            print(f"Tabela {nome_tabela} já existe!", end='\r')

# %%
with open('./queries/atividade_1.sql', 'r', encoding='utf8') as disconto:
    query = disconto.read()

# %%
df_vendas_desconto = pd.read_sql(query, engine)
df_vendas_desconto.to_parquet(fr'.{caminho_final}\vendas_desconto.parquet', engine='fastparquet', compression='snappy')


