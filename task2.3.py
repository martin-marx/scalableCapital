import duckdb
import datetime
import os

from constants import names_table_name

initial_dates = [datetime.datetime(2019, 1, 10),
                 datetime.datetime(2019, 1, 11),
                 datetime.datetime(2019, 1, 12),
                 datetime.datetime(2019, 3, 12),
                 datetime.datetime(2019, 4, 12)]


# 2.3
def get_the_result_script(initial_date):
    dates = [initial_date - datetime.timedelta(days=x) for x in range(7)]

    existing_data_queries = [
        f"SELECT user_name FROM read_parquet('{names_table_name}/year={date.year}/month={date.month}/day={date.day}/*.parquet')"
        for date
        in dates
        if os.path.isdir(f"{names_table_name}/year={date.year}/month={date.month}/day={date.day}")]

    existing_data_query = "\nUNION\n".join(existing_data_queries)

    if not existing_data_query:
        write_query = f"""SELECT (DATE '{initial_date}') AS date, 0 AS number_active_users, 0 AS percentage_active_users"""
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
      (DATE '{initial_date}') AS date ,
      active_user_count AS number_active_users,
      (active_user_count / all_user_count) AS percentage_active_users
    FROM
      active_user_counter, all_users_counter
    ORDER BY 
      date
    """

    return write_query


if __name__ == '__main__':

    queries = [f"""({get_the_result_script(initial_date)})""" for initial_date in initial_dates]
    if queries:
        combined_query = "\nUNION ALL\n".join(queries)
        final_query = f"""
              WITH data AS (
                {combined_query}
              )
              SELECT
                date,
                number_active_users,
                percentage_active_users
              FROM
                data
              ORDER BY
                date
        """
        duckdb.sql(final_query).show()
