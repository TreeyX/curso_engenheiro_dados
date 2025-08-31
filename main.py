# %%
import pandas as pd
import yaml
import os
from sqlalchemy import create_engine, text

# %%
with open('./config.yaml', 'r', encoding='utf-8') as arquivo_configuracao:
    config = yaml.safe_load(arquivo_configuracao)

postgres_config = config['credenciais_postgres']

# %%
connection_string = f"postgresql+psycopg2://{postgres_config['user']}:{postgres_config['password']}@{postgres_config['host']}:{postgres_config['port']}/{postgres_config['database']}"
engine = create_engine(connection_string)

# %%
with open('./ddl/create_table.sql', 'r', encoding='utf8') as criar_tabela:
    query = criar_tabela.read()

with engine.connect() as conn:
    conn.execute(text(query))
    conn.commit

# %%
with open('./queries/atividade_1.sql', 'r', encoding='utf8') as disconto:
    query = disconto.read()

# %%
df_vendas_desconto = pd.read_sql(query, engine)
df_vendas_desconto.to_parquet(fr'.\data\etl_data\vendas_desconto.parquet', engine='fastparquet', compression='snappy')


