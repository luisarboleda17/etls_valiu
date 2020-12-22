
import sys
import argparse
from pyspark.sql import SparkSession, Window
from pyspark.sql.types import DateType, DecimalType, IntegerType, StructField, StructType
import pyspark.sql.functions as F


def run(argv: sys.argv):
    parser = argparse.ArgumentParser()
    parser.add_argument('--project', dest='project', type=str)
    parser.add_argument('--bucket', dest='bucket', type=str)
    parser.add_argument('--core_table', dest='core_table', type=str)
    parser.add_argument('--results_table', dest='results_table', type=str)
    known_args, pipeline_args = parser.parse_known_args(argv)

    # Known args
    # Project - Google Cloud project
    # Bucket table - Bucket for temporal table creation
    # Core table - Core transaction table name
    # Results table - Results retention table name
    project = known_args.project
    bucket = known_args.bucket
    core_table = known_args.core_table
    results_table = known_args.results_table

    spark = SparkSession\
        .builder\
        .appName(f'spark-etl_{core_table}')\
        .getOrCreate()
    spark.conf.set('temporaryGcsBucket', bucket)

    # Read source transactions Dataframe
    transactions = spark.read.format('bigquery')\
        .option('table', f'{project}:core.{core_table}')\
        .load()\
        .where(F.col('created_at').isNotNull())
    transactions.createOrReplaceTempView(core_table)

    # Get starting date of users
    users_date = transactions\
        .select('user_id', 'created_at')\
        .groupBy('user_id')\
        .agg({'created_at': 'min'})\
        .withColumnRenamed('min(created_at)', 'min_date')\
        .withColumn('min_date', F.to_date(F.col('min_date')))\
        .orderBy('user_id', ascending=True)

    # Get transactions dates
    transactions_dates = transactions\
        .select('created_at')\
        .withColumnRenamed('created_at', 'date')\
        .withColumn('date', F.to_date(F.col('date')))\
        .distinct()\
        .orderBy('date', ascending=True)

    # Get user by day
    day_users = transactions\
        .select('user_id', 'created_at')\
        .withColumn('created_at', F.to_date(F.col('created_at')))\
        .groupBy('created_at')\
        .count()\
        .withColumnRenamed('created_at', 'date')\
        .withColumnRenamed('count', 'total_users')\
        .orderBy('date', ascending=False)

    # Get new users of transactions days
    transactions_new = transactions_dates.alias('d')\
        .join(users_date.alias('u'), F.col('d.date') == F.col('u.min_date'), how='left')\
        .select('date', 'user_id')\
        .groupBy('date') \
        .count() \
        .withColumnRenamed('count', 'new_users')

    # Join total and new users count by day
    transactions_dates = transactions_dates.alias('c')\
        .join(transactions_new.alias('n'), 'date')\
        .join(day_users.alias('t'), 'date')\
        .orderBy('date', ascending=False)

    # Calculate retention by date
    retention_window = Window.orderBy('date').rowsBetween(-1, -1)
    retentions_day = transactions_dates\
        .withColumn('prev_total_users', F.lag('total_users', 1, 0).over(retention_window))\
        .withColumn(
            'retention_rate',
            (F.col('total_users') - F.col('new_users')) / F.lag('total_users', 1, 0).over(retention_window)
        )\
        .withColumn('retention_rate', F.col('retention_rate').cast('decimal(38, 9)'))\
        .filter(F.col('date').isNotNull())\
        .filter(F.col('new_users').isNotNull())\
        .filter(F.col('total_users').isNotNull())\
        .filter(F.col('retention_rate').isNotNull())

    # Set required fields for schema - Preparing to write in BigQuery table
    retention_schema = [
        StructField('date', DateType(), False),
        StructField('new_users', IntegerType(), False),
        StructField('total_users', IntegerType(), False),
        StructField('prev_total_users', IntegerType(), False),
        StructField('retention_rate', DecimalType(38, 9), False)
    ]
    retention_df = spark.createDataFrame(retentions_day.rdd, StructType(retention_schema))

    # Write results in BigQuery
    retention_df.write\
        .format('bigquery')\
        .option('table', f'{project}:results.{results_table}')\
        .mode("append")\
        .save()


if __name__ == '__main__':
    run(sys.argv)
