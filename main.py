# %%
import pandas as pd
import yaml
import os
from pathlib import Path
from sqlalchemy import create_engine, text
from sqlalchemy.exc import ProgrammingError

# %%
with open('./config.yaml', 'r', encoding='utf-8') as arquivo_configuracao:
    config = yaml.safe_load(arquivo_configuracao)

postgres_config = config['credenciais_postgres']
caminho_final = fr".\data\etl_data"

# %%
connection_string = f"postgresql+psycopg2://{postgres_config['user']}:{postgres_config['password']}@{postgres_config['host']}:{postgres_config['port']}/{postgres_config['database']}"
engine = create_engine(connection_string)

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
    arquivo_csv = caminho_arquivos_raw / f"{nome_tabela}.csv"

    with engine.begin() as conn:
        try:
            resultado = conn.execute(text(f"select 1 from {nome_tabela} limit 1")).fetchone()
            tabela_existe = True
        except ProgrammingError:
            tabela_existe = False

        if not tabela_existe:
            df = pd.read_csv(arquivo_csv, sep=';', encoding='utf-8')
            df.to_sql(nome_tabela, conn, if_exists='append', index=False)
            print(f"Tabela {nome_tabela} não existe, criando!")
        else:
            print(f"Tabela {nome_tabela} já existe!")

# %%
lista_tarefas = ['vendas_desconto', 'performance_vendedores', 'top_produtos_caros', 'vendas_ultimos_anos']
for tarefas in lista_tarefas:

    with open(f'./queries/{tarefas}.sql', 'r', encoding='utf8') as arquivo_sql:
        query = arquivo_sql.read()
        df = pd.read_sql(query, engine)

    arquivo_parquet = Path(caminho_final) / f"{tarefas}.parquet"
    df.to_parquet(arquivo_parquet, engine='fastparquet', compression='snappy')

    print(f'gerado {tarefas}.parquet') 


