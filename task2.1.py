import duckdb

con = duckdb.connect("listenings_test.db")
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
con.sql(write_query).show()

# 2.1.2
write_query = f"""
SELECT 
  COUNT(distinct user_name) AS listeners_number
FROM
  read_parquet('{result_table_name}/**/*.parquet', hive_partitioning = 1)
WHERE 
  year = 2023 AND month = 11 AND day = 29 
"""
con.sql(write_query).show()

# 2.1.3(version 1)
write_query = f"""
WITH ranked AS (
  SELECT  
    RANK() OVER (PARTITION BY user_name ORDER BY time ASC) AS row_number,
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
  row_number = 1
"""
con.sql(write_query).show()

# 2.1.3(version 2)
write_query = f"""
WITH max_time AS (
  SELECT  
    MIN(time) AS time,
    user_name
  FROM
    read_parquet('{result_table_name}/**/*.parquet')
  GROUP BY
    user_name
)
SELECT 
  data.user_name, 
  data.song_name 
FROM
  read_parquet('{result_table_name}/**/*.parquet') AS data
INNER JOIN
  max_time
ON data.user_name = max_time.user_name AND data.time = max_time.time
"""
con.sql(write_query).show()
