import duckdb

con = duckdb.connect("listenings_test.db")
result_table_name = 'listenings_facts'

# 2.2
write_query = f"""
WITH counters AS (
  SELECT  
    COUNT(user_name) AS listens,
    user_name,
    DATE_TRUNC('day', time) as date
  FROM
    read_parquet('{result_table_name}/**/*.parquet')
  GROUP BY
    date, user_name
),
ranked AS (
  SELECT
    ROW_NUMBER() OVER (PARTITION BY user_name, date ORDER BY listens DESC) AS ranked_listens,
    listens,
    user_name,
    date
  FROM
    counters
)
SELECT 
  user_name, 
  listens,
  date 
FROM
  ranked
WHERE 
  ranked_listens <= 3
ORDER BY
  user, listens
"""
con.sql(write_query).show()
