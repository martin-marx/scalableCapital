import duckdb
import hashlib
import uuid

# con = duckdb.connect("music.db")
day = 29
month = 11
year = 2023

read_date = f"{year}-{month}-{day}"
read_data_file_name = f"{read_date}.json"
read_data_file_path = f"resources/{read_data_file_name}"
read_new_data_script = f"SELECT * FROM '{read_data_file_path}'"
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

# res2 = con.sql(f"select * from {result_table_name}")
# print(res2)

# Writing section
# -----------------
prepare_to_write_query = f"SELECT song as song_name, user as user_name, time as time, datetrunc('day', time) as date, md5(concat(song, user, time)) as hash FROM '{read_data_file_path}'"
write_query = f"""COPY ({prepare_to_write_query}) TO '{result_table_name}' (FORMAT PARQUET, PARTITION_BY (date), OVERWRITE_OR_IGNORE, FILENAME_PATTERN "listenings_{uuid.uuid4()}")"""

# con.sql(write_query)

# Reading to check section
# -------------------
# read_parquet_query = f"SELECT * from read_parquet('{result_table_name}/date={read_date}/*.parquet')"
read_parquet_query = f"SELECT * from read_parquet('{result_table_name}/*/*.parquet')"

written_res = con.sql(read_parquet_query)
print(written_res)

# 6181c096ea20f44c6a7af00583a00e53
# c61abd23bd73b4740c40e63b2ce68ba3
# f5fd2a76c259c0bde5838b5ccfd33fe5