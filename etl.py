import duckdb
import uuid
import os
import shutil

from constants import read_data_file_path, result_table_name, names_table_name

columns_to_read = """
     {
       track_metadata: 'STRUCT(
         track_name VARCHAR
       )',
       listened_at: 'INT64',   
       user_name: 'VARCHAR',
       recording_msid: 'UUID'
    }"""

table_to_read = f"""
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
  track_metadata.track_name AS song_name, 
  user_name AS user_name,
  recording_msid AS recording_msid, 
  TO_TIMESTAMP(listened_at) AT TIME ZONE 'Germany/Berlin' AS time,
  HASH(CONCAT(listened_at, recording_msid)) AS hash, 
  YEAR(TO_TIMESTAMP(listened_at)) AS year, 
  MONTH(TO_TIMESTAMP(listened_at)) AS month, 
  DAY(TO_TIMESTAMP(listened_at)) AS day 
FROM {table_to_read}
"""


def get_new_data_dates():
    get_write_dates_query = f"SELECT DISTINCT DATETRUNC('day', TO_TIMESTAMP(listened_at)) AS date FROM {table_to_read}"
    return duckdb.sql(get_write_dates_query).fetchall()


def save_data(dates):
    existing_data_queries = [
        f"SELECT hash FROM read_parquet('{result_table_name}/year={date.year}/month={date.month}/day={date.day}/*.parquet')"
        for date,
        in dates
        if os.path.isdir(f"{result_table_name}/year={date.year}/month={date.month}/day={date.day}")]

    if existing_data_queries:
        hashes_query = "\nUNION ALL\n".join(existing_data_queries)
        new_data_query = f"""
        WITH new_data AS (
          {read_query}
        ),
        existing_hashes AS (
          {hashes_query}
        )
        SELECT 
          new_data.* 
        FROM 
          new_data 
        ANTI JOIN 
          existing_hashes ON new_data.hash = existing_hashes.hash
      """
    else:
        new_data_query = f"{read_query}"

    write_query = f"""COPY ({new_data_query}) TO '{result_table_name}' (FORMAT PARQUET, PARTITION_BY (year, month, day), OVERWRITE_OR_IGNORE, FILENAME_PATTERN "listenings_{uuid.uuid4()}")"""
    duckdb.sql(write_query)


def read_results():
    read_parquet_query = f"SELECT * FROM read_parquet('{result_table_name}/**/*.parquet')"
    duckdb.sql(read_parquet_query).show()
    read_parquet_query = f"SELECT COUNT(*) FROM read_parquet('{result_table_name}/**/*.parquet')"
    duckdb.sql(read_parquet_query).show()


def recalculate_names(dates):
    recalc_read_names_queries = [
        f"SELECT DISTINCT user_name, year, month, day FROM read_parquet('{result_table_name}/year={date.year}/month={date.month}/day={date.day}/*.parquet')"
        for date,
        in dates]

    for date, in dates:
        if os.path.isdir(f"{names_table_name}/year={date.year}/month={date.month}/day={date.day}"):
            shutil.rmtree(f"""{names_table_name}/year={date.year}/month={date.month}/day={date.day}""")

    for recalc_read_names_query in recalc_read_names_queries:
        recalc_query = f"""COPY ({recalc_read_names_query}) TO '{names_table_name}' 
            (FORMAT PARQUET, PARTITION_BY (year, month, day), OVERWRITE_OR_IGNORE, FILENAME_PATTERN "counters")"""
        duckdb.sql(recalc_query)


def read_name_results():
    read_parquet_query = f"SELECT * FROM read_parquet('{names_table_name}/**/*.parquet')"
    duckdb.sql(read_parquet_query).show()


if __name__ == '__main__':
    dates = get_new_data_dates()
    save_data(dates)
    read_results()
    recalculate_names(dates)
    read_name_results()
