import duckdb
import uuid
import os

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
counters_table_name = 'counters'

# con = duckdb.connect("listenings_test.db")
# con.sql(f"""
# CREATE TABLE {result_table_name} (
#     song_name    VARCHAR,
#     user_name    VARCHAR,
#     time         TIMESTAMP,
#     hash         VARCHAR,
#     year         SMALLINT,
#     month        SMALLINT,
#     day          SMALLINT
# );""")

# con.sql(f"""
# CREATE TABLE {counters_table_name} (
#     user_name    VARCHAR,
#     year         SMALLINT,
#     month        SMALLINT,
#     day          SMALLINT
# );""")
# con.sql("DROP TABLE counters")

# Writing section
# -----------------
prepare_to_write_query = f"SELECT song as song_name, user as user_name, time as time, md5(concat(song, user, time)) as hash, year(time) as year, month(time) as month, day(time) as day  FROM '{read_data_file_path}'"
write_query = f"""COPY ({prepare_to_write_query}) TO '{result_table_name}' (FORMAT PARQUET, PARTITION_BY (year, month, day), OVERWRITE_OR_IGNORE, FILENAME_PATTERN "listenings_{uuid.uuid4()}")"""
# con.sql(write_query)


# Establishing of idempotency
# -------------------
get_write_dates_query = f"SELECT DISTINCT datetrunc('day', time) as date FROM '{read_data_file_path}'"
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
    new_data_query = f"""WITH new_data AS (
    SELECT song as song_name, user as user_name, time as time, md5(concat(song, user, time)) as hash, year(time) as year, month(time) as month, day(time) as day FROM '{read_data_file_path}'
    ),
    existing_hashes AS (
    {hashes_query}
    )
    SELECT new_data.* from new_data ANTI JOIN existing_hashes ON new_data.hash = existing_hashes.hash
  """
    duckdb.sql(new_data_query).show()
else:
    new_data_query = f"""SELECT song as song_name, user as user_name, time as time, md5(concat(song, user, time)) as hash, year(time) as year, month(time) as month, day(time) as day FROM '{read_data_file_path}'"""

write_query = f"""COPY ({new_data_query}) TO '{result_table_name}' (FORMAT PARQUET, PARTITION_BY (year, month, day), OVERWRITE_OR_IGNORE, FILENAME_PATTERN "listenings_{uuid.uuid4()}")"""
duckdb.sql(write_query)

# Reading to check section
read_parquet_query = f"SELECT * FROM read_parquet('{result_table_name}/**/*.parquet')"
duckdb.sql(read_parquet_query).show()


recalc_read_names_queries = [
    f"SELECT distinct user_name, year, month, day FROM read_parquet('{result_table_name}/year={date.year}/month={date.month}/day={date.day}/*.parquet')"
    for date,
    in dates]

for recalc_read_names_query in recalc_read_names_queries:
    recalc_query = f"""COPY ({recalc_read_names_query}) TO '{counters_table_name}' (FORMAT PARQUET, PARTITION_BY (year, month, day), OVERWRITE_OR_IGNORE, FILENAME_PATTERN "counters")"""
    duckdb.sql(recalc_query)

# Reading to check section
read_parquet_query = f"SELECT * FROM read_parquet('{counters_table_name}/**/*.parquet')"
duckdb.sql(read_parquet_query).show()
