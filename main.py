# %%
import pandas as pd
import psycopg2
import yaml
from sqlalchemy import create_engine

# %%
with open('./config.yaml', 'r', encoding='utf-8') as arquivo_configuracao:
    config = yaml.safe_load(arquivo_configuracao)

postgres_config = config['credenciais_postgres']

with open('./queries/atividade_1.sql', 'r', encoding='utf8') as disconto:
    query = disconto.read()

# %%
connection_string = f"postgresql+psycopg2://{postgres_config['user']}:{postgres_config['password']}@{postgres_config['host']}:{postgres_config['port']}/{postgres_config['database']}"
engine = create_engine(connection_string)

# %%
df_vendas_desconto = pd.read_sql(query, engine)
df_vendas_desconto.to_parquet(fr'.\etl_data\vendas_desconto.parquet', engine='pyarrow', compression='snappy')

