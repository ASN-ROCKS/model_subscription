# Databricks notebook source
# MAGIC %pip install tqdm

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

from databricks.feature_engineering import FeatureEngineeringClient
import datetime
from tqdm import tqdm

def import_query(path):
    with open(path, "r") as open_file:
        query = open_file.read()
    return query


def table_exists(database, table):
    return (spark.sql(f'SHOW TABLES FROM {database}')
                .filter(f"tableName = '{table}'")
                .count()) > 0
    
def get_dates(start, stop):
    dates = []
    while start <= stop:
        dates.append(start)
        start_date = datetime.datetime.strptime(start, '%Y-%m-%d')
        start_date += datetime.timedelta(days=1)
        start = start_date.strftime("%Y-%m-%d")

    dates = [i for i in dates if i.endswith("01")]
    return dates

# COMMAND ----------

config = {
    "gameplay":
        {
            "name":"gold.gamersclub.gameplay",
            "description":"Feature Store da Gamers Club em relação à jogadores e suas estatísticas de partidas",
            "primary_keys":["dtRef","idJogador"],
            "timeseries_columns":["dtRef"]
        },
    
    "sales":
        {
            "name":"gold.gamersclub.sales",
            "description":"Feature Store da Gamers Club em relação às compras dos jogadores",
            "primary_keys":["dtRef","idJogador"],
            "timeseries_columns":["dtRef"]
        },
}

tabela = 'gameplay'
dt_start = '2021-10-01'
dt_stop = '2022-01-01'
dates = get_dates(dt_start, dt_stop)

config_table = config[tabela]
config_table

# COMMAND ----------

query = import_query(f"{tabela}.sql")

fe = FeatureEngineeringClient()

if not table_exists('gold.gamersclub', tabela):
    query_format = query.format(date=dates.pop(0))
    df = spark.sql(query_format)

    fe.create_table(
        name=config_table['name'],
        description=config_table['description'],
        primary_keys=config_table['primary_keys'],
        timeseries_columns=config_table['timeseries_columns'],
        schema=df.schema,
        df=df,
    )

for i in tqdm(dates):
    print(f"Inserindo novos dados na tabela... {i}")
    query_format = query.format(date=i)
    df = spark.sql(query_format)
    fe.write_table(
        name=config_table['name'],
        df=df,
        mode="merge")
    print("ok")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT count(*),
# MAGIC        count(distinct dtRef, idJogador),
# MAGIC        count(distinct idJogador)
# MAGIC FROM gold.gamersclub.gameplay
