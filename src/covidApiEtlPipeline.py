import json
from datetime import datetime

import findspark
import mysql.connector
import requests
from mysql.connector import Error
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from conf.config import Config
from schema import us_covid


def main():
    # setup db
    cursor, connection = get_db_connection()
    setup_db()

    # initialize spark
    spark = initialize_spark()

    # get data from api
    json_data = poll_api(Config.STATES, Config.DATES)

    # create dataframe from rdd of json data
    rdd = spark.sparkContext.parallelize([json_data], 4).map(lambda x: json.dumps(x))
    df = spark.read.json(rdd, schema=us_covid.source_api_schema)

    # rename columns and clean data
    df = df.toDF(*us_covid.mysql_schema)
    df = clean_data(df)

    # remove duplicates if applicable
    unique_keys = get_unique_keys('us_covid', cursor)
    if unique_keys is not None:
        new_df = df.filter(~col('hash').isin(unique_keys))
        df_delta = df.count() - new_df.count()
        print(f"Deduped {df_delta} rows from DF", "\n")
    else:
        new_df = df

    # insert into MySql table
    write_df_to_mysql(new_df, 'us_covid')

    # cleanup connections
    cursor.close()
    connection.close()


def get_db_connection():
    connection = mysql.connector.connect(host=Config.MYSQL_HOST,
                                         port=Config.MYSQL_PORT,
                                         database=Config.MYSQL_DB,
                                         user=Config.MYSQL_USER)

    try:
        if connection.is_connected():
            db_info = connection.get_server_info()
            print("Connected to MySQL Server version ", db_info)
            cursor = connection.cursor()
            create_table(cursor, 'us_covid')
            return cursor, connection
    except Error as e:
        print("Error while connecting to MySQL", e)


def setup_db():
    cursor = get_db_connection()
    create_table(cursor, 'us_covid')


def create_table(cursor, table):
    # Open and read the file as a single buffer
    file = open(f'sql/create_table_{table}.sql', 'r')
    sql = file.read()
    file.close()

    try:
        cursor.execute(sql)
        print(f"Created table {table} in MySql db {Config.MYSQL_DB}", "\n")
    except:
        print(f"MySql table: {table} already exists!", "\n")


def initialize_spark():
    mysql_jar = findspark.add_packages('mysql:mysql-connector-java:8.0.25')

    s = SparkSession.builder \
        .master("local[*]") \
        .appName("US Covid Pipeline") \
        .config("spark.driver.extraClassPath", mysql_jar) \
        .config("spark.debug.maxToStringFields=200") \
        .getOrCreate()
    print("Spark Initialized", "\n")
    return s


def poll_api(states, dates):
    responses = []

    print(f"Polling api for dates: {dates} and states {states} ")
    start_time = datetime.now()
    for date in dates:
        for state in states:
            try:
                responses.append(requests.get(f'{Config.API_BASE_URI}/{state}/{date}.json').json())
            except Error as e:
                print("Error reaching api", e)
    print(f"Retrieved {len(responses)} from {Config.API_BASE_URI} in {datetime.now() - start_time} seconds")

    return responses


def clean_data(_df):
    # remove unused fields
    df_dropped = _df.drop("fips")

    # filter rows with poor data quality
    df_tmp = df_dropped.fillna("n/a", subset="data_quality_grade")
    df_filtered = df_tmp.filter(col('data_quality_grade') != 'Serious issues exist')
    df_delta = df_dropped.count() - df_filtered.count()
    if df_delta > 0:
        print(f"Dropped {df_delta} rows due to poor data quality from source", "\n")

    return df_filtered


def get_unique_keys(table, _cursor):
    sql_statement = f"SELECT DISTINCT(hash) from {Config.MYSQL_DB}.{table};"
    cursor, _ = get_db_connection()
    cursor.execute(sql_statement)
    keys = cursor.fetchall()
    unique_keys = []
    for x in keys:
        unique_keys.append(x[0])

    return unique_keys


def write_df_to_mysql(_df, table):
    try:
        start_time = datetime.now()
        _df.write.format('jdbc').options(
            url=f'jdbc:mysql://{Config.MYSQL_HOST}/{Config.MYSQL_DB}',
            port=f'{Config.MYSQL_PORT}',
            driver='com.mysql.cj.jdbc.Driver',
            dbtable=f'{table}',
            user=f'{Config.MYSQL_USER}'
        ).mode('append').save()
    except Error as e:
        print("Error while writing DF to MySQL", e)
    finally:
        print(
            f"Inserted {_df.count()} rows into {Config.MYSQL_DB}.{table} in {datetime.now() - start_time} seconds")


if __name__ == '__main__':
    main()
