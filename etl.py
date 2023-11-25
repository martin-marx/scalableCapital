import duckdb
import hashlib
import uuid
import pandas as pd

# con = duckdb.connect("music.db")

# Single day data read variables
# ------------------
day = 29
month = 11
year = 2023
read_date = f"{year}-{month}-{day}"
read_data_file_name = f"{read_date}.json"
# read_data_file_path = f"resources/{read_data_file_name}"
# read_new_data_script = f"SELECT * FROM '{read_data_file_path}'"

# Multi day data read variables
# ------------------
read_data_file_path = f"resources/*.json"



result_table_name = 'listenings_facts'

hasher = hashlib.new('sha256', usedforsecurity=False)

# duckdb.read_json(f"resources/{date}.json")
# res = duckdb.sql(read_new_data_script)
# print(res)

con = duckdb.connect("listenings_test.db")
# con.sql(f"""
# CREATE TABLE {result_table_name} (
#     song_name    VARCHAR,
#     user_name    VARCHAR,
#     time       TIMESTAMP,
#     date            DATE,
#     hash         VARCHAR
# );""")

# res2 = con.sql(f"select * FROM {result_table_name}")
# print(res2)

# Writing section
# -----------------
prepare_to_write_query = f"SELECT song as song_name, user as user_name, time as time, datetrunc('day', time) as date, md5(concat(song, user, time)) as hash FROM '{read_data_file_path}'"
write_query = f"""COPY ({prepare_to_write_query}) TO '{result_table_name}' (FORMAT PARQUET, PARTITION_BY (date), OVERWRITE_OR_IGNORE, FILENAME_PATTERN "listenings_{uuid.uuid4()}")"""
# con.sql(write_query)






# Establishing of idempotency
# -------------------
get_write_dates_query = f"SELECT DISTINCT datetrunc('day', time) as date FROM '{read_data_file_path}'"
dates = con.sql(get_write_dates_query).fetchall()
existing_data_queries = [f"SELECT hash FROM read_parquet('{result_table_name}/date={date.year}-{date.month}-{date.day}/*.parquet')" for date, in dates]
print(existing_data_queries)
print("\n")

hashes_query = "\nUNION ALL\n".join(existing_data_queries)
# existing_hashes = con.sql(hashes_query)
# print(existing_hashes)

new_data_query = f"""WITH new_data AS (
SELECT song as song_name, user AS user_name, time, datetrunc('day', time) AS date, md5(concat(song, user, time)) AS hash FROM '{read_data_file_path}'
),
existing_hashes AS (
{hashes_query}
)
SELECT new_data.* from new_data ANTI JOIN existing_hashes ON new_data.hash = existing_hashes.hash
"""
new_data = con.sql(new_data_query)
print(new_data)

write_query = f"""COPY ({new_data_query}) TO '{result_table_name}' (FORMAT PARQUET, PARTITION_BY (date), OVERWRITE_OR_IGNORE, FILENAME_PATTERN "listenings_{uuid.uuid4()}")"""
con.sql(write_query)

# Reading to check section
# -------------------
# read_parquet_query = f"SELECT * FROM read_parquet('{result_table_name}/date={read_date}/*.parquet')"
read_parquet_query = f"SELECT * FROM read_parquet('{result_table_name}/*/*.parquet')"
written_res = con.sql(read_parquet_query)
print(written_res)


# 6181c096ea20f44c6a7af00583a00e53
# c61abd23bd73b4740c40e63b2ce68ba3
# f5fd2a76c259c0bde5838b5ccfd33fe5