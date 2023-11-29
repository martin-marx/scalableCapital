import duckdb

result_table_name = 'listenings_facts'

# 2.1.1
write_query = f"""
SELECT 
  COUNT(user_name) AS songs_number,
  user_name
FROM
  read_parquet('{result_table_name}/**/*.parquet')
GROUP BY 
  user_name
ORDER BY 
  COUNT(user_name) DESC
LIMIT 10
"""
duckdb.sql(write_query).show()

# 2.1.2
write_query = f"""
SELECT 
  COUNT(DISTINCT user_name) AS listeners_number
FROM
  read_parquet('{result_table_name}/**/*.parquet', hive_partitioning = 1)
WHERE 
  year = 2019 AND month = 3 AND day = 1 
"""
duckdb.sql(write_query).show()

# 2.1.3
write_query = f"""
WITH ranked AS (
  SELECT  
    ROW_NUMBER() OVER (PARTITION BY user_name ORDER BY time ASC) AS rank,
    user_name,
    song_name
  FROM
    read_parquet('{result_table_name}/**/*.parquet')
)
SELECT 
  user_name, 
  song_name 
FROM
  ranked
WHERE 
  rank = 1
ORDER BY
  user_name
"""
duckdb.sql(write_query).show()
