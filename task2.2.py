import duckdb

from constants import result_table_name

if __name__ == '__main__':

    # 2.2
    write_query = f"""
    WITH counters AS (
      SELECT  
        COUNT(user_name) AS number_of_listens,
        user_name,
        DATE_TRUNC('day', time) as date
      FROM
        read_parquet('{result_table_name}/**/*.parquet')
      GROUP BY
        date, user_name
    ),
    ranked AS (
      SELECT
        ROW_NUMBER() OVER (PARTITION BY user_name ORDER BY number_of_listens DESC) AS ranked_listens,
        number_of_listens,
        user_name,
        date
      FROM
        counters
    )
    SELECT 
      user_name, 
      number_of_listens,
      date
    FROM
      ranked
    WHERE 
      ranked_listens <= 3
    ORDER BY
      user, number_of_listens
    """
    duckdb.sql(write_query).show()
