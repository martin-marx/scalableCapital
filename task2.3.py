import duckdb
import datetime
import os

names_table_name = 'names'
result_table_name = 'listenings_facts'

# today = datetime.date.today()
today = datetime.datetime(2019, 1, 10)
dates = [today - datetime.timedelta(days=x) for x in range(7)]

existing_data_queries = [
    f"SELECT user_name FROM read_parquet('{names_table_name}/year={date.year}/month={date.month}/day={date.day}/*.parquet')"
    for date
    in dates
    if os.path.isdir(f"{names_table_name}/year={date.year}/month={date.month}/day={date.day}")]

existing_data_query = "\nUNION\n".join(existing_data_queries)

# 2.3
if not existing_data_query:
    write_query = f"""SELECT (DATE '{today}') AS date, 0 AS number_active_users, 0 AS percentage_active_users"""
else:
    write_query = f"""
  WITH active_users AS (
    {existing_data_query}
  ),
  active_user_counter AS (
    SELECT 
      COUNT(DISTINCT user_name) AS active_user_count
    FROM
      active_users
  ),
  all_users_counter AS (
    SELECT  
      COUNT(DISTINCT user_name) AS all_user_count 
    FROM 
      read_parquet('{names_table_name}/**/*.parquet')
  )
  SELECT 
    (DATE '{today}') AS date ,
    active_user_count AS number_active_users,
    (active_user_count / all_user_count) AS percentage_active_users
  FROM
    active_user_counter, all_users_counter
  ORDER BY 
    date
  """
duckdb.sql(write_query).show()
