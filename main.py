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

# %%
connection_string = f"postgresql+psycopg2://{postgres_config['user']}:{postgres_config['password']}@{postgres_config['host']}:{postgres_config['port']}/{postgres_config['database']}"
engine = create_engine(connection_string)
raw_engine = engine.raw_connection()
insp = inspect(engine)

# %%
with open('./ddl/create_table.sql', 'r', encoding='utf8') as criar_tabela:
    query = criar_tabela.read()

with engine.connect() as conn:
    conn.execute(text(query))
    conn.commit

# %%
caminho_arquivos_raw = Path(fr".\data\raw_data")
lista_arquivos_raw = list(caminho_arquivos_raw.glob("*.csv"))

for arquivos in lista_arquivos_raw:
    arquivo = os.path.basename(arquivos)
    nome_tabela = arquivo.split(".csv")[0]
    if not insp.has_table(nome_tabela, schema='public'):

        with raw_engine.cursor() as cur:
            with open(fr'.\data\raw_data\{arquivo}', 'r', encoding='utf-8') as raw_file:
                cur.copy_expert(f"COPY {nome_tabela} FROM STDIN WITH DELIMITER ';' CSV HEADER", raw_file)
        raw_engine.commit()
    else:
        print('Sem tabelas novas!', end='\r')

raw_engine.close()

# %%
with open('./queries/atividade_1.sql', 'r', encoding='utf8') as disconto:
    query = disconto.read()

# %%
df_vendas_desconto = pd.read_sql(query, engine)
df_vendas_desconto.to_parquet(fr'.\data\etl_data\vendas_desconto.parquet', engine='fastparquet', compression='snappy')


