import duckdb
import uuid
import os

read_data_file_path = f"resources/dataset.txt"

result_table_name = 'listenings_facts'
counters_table_name = 'counters'

columns_to_read = """
     {
       track_metadata: 'STRUCT(
         track_name VARCHAR
       )',
       listened_at: 'INT64',   
       user_name: 'VARCHAR',
       recording_msid: 'UUID'
    }"""

read_table = f"""
read_json(
    '{read_data_file_path}',
    columns = {columns_to_read},
    auto_detect=false, 
    format='newline_delimited',
    ignore_errors = true
    )
"""

read_query = f"""
SELECT 
  track_metadata.track_name,
  listened_at, 
  user_name, 
  recording_msid 
FROM 
  {read_table}
"""



# Establishing of idempotency
get_write_dates_query = f"SELECT DISTINCT DATETRUNC('day', TO_TIMESTAMP(listened_at)) AS date FROM {read_table}"
dates = duckdb.sql(get_write_dates_query).fetchall()
existing_data_queries = [
    f"SELECT hash FROM read_parquet('{result_table_name}/year={date.year}/month={date.month}/day={date.day}/*.parquet')"
    for date,
    in dates
    if os.path.isdir(f"{result_table_name}/year={date.year}/month={date.month}/day={date.day}")]
# print(existing_data_queries)
# print("\n")

if existing_data_queries:
    hashes_query = "\nUNION ALL\n".join(existing_data_queries)
    new_data_query = f"""
    WITH new_data AS (
      SELECT 
        track_metadata.track_name AS song_name, 
        user_name AS user_name,
        recording_msid AS recording_msid, 
        TO_TIMESTAMP(listened_at) AS time,
        HASH(CONCAT(listened_at, recording_msid)) AS hash, 
        YEAR(TO_TIMESTAMP(listened_at)) AS year, 
        MONTH(TO_TIMESTAMP(listened_at)) AS month, 
        DAY(TO_TIMESTAMP(listened_at)) AS day 
      FROM {read_table}
    ),
    existing_hashes AS (
      {hashes_query}
    )
    SELECT 
      new_data.* 
    FROM 
      new_data ANTI JOIN existing_hashes ON new_data.hash = existing_hashes.hash
  """
    duckdb.sql(new_data_query).show()
else:
    new_data_query = f"""
    SELECT 
        track_metadata.track_name AS song_name, 
        user_name AS user_name,
        recording_msid AS recording_msid, 
        TO_TIMESTAMP(listened_at) AS time,
        HASH(CONCAT(listened_at, recording_msid)) AS hash, 
        YEAR(TO_TIMESTAMP(listened_at)) AS year, 
        MONTH(TO_TIMESTAMP(listened_at)) AS month, 
        DAY(TO_TIMESTAMP(listened_at)) AS day 
      FROM {read_table}
"""

write_query = f"""COPY ({new_data_query}) TO '{result_table_name}' (FORMAT PARQUET, PARTITION_BY (year, month, day), OVERWRITE_OR_IGNORE, FILENAME_PATTERN "listenings_{uuid.uuid4()}")"""


duckdb.sql(write_query)

# Reading to check section
read_parquet_query = f"SELECT * FROM read_parquet('{result_table_name}/**/*.parquet')"
duckdb.sql(read_parquet_query).show()


recalc_read_names_queries = [
    f"SELECT DISTINCT user_name, year, month, day FROM read_parquet('{result_table_name}/year={date.year}/month={date.month}/day={date.day}/*.parquet')"
    for date,
    in dates]

for recalc_read_names_query in recalc_read_names_queries:
    recalc_query = f"""COPY ({recalc_read_names_query}) TO '{counters_table_name}' (FORMAT PARQUET, PARTITION_BY (year, month, day), OVERWRITE_OR_IGNORE, FILENAME_PATTERN "counters")"""
    duckdb.sql(recalc_query)

# Reading to check section
read_parquet_query = f"SELECT * FROM read_parquet('{counters_table_name}/**/*.parquet')"
duckdb.sql(read_parquet_query).show() #7358

