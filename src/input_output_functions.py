# Databricks notebook source
import pyspark.sql.functions as F 
from pyspark.sql.types import *
import os

if "DATABRICKS_RUNTIME_VERSION" not in os.environ:
  from pyspark.sql import SparkSession
  def get_spark():
      """Provide Sparksession to module."""

      spark = (
          SparkSession.builder
              .master("local")
              .appName("TFL Notebook")
              .config('spark.executor.memory', '8G')
              .config('spark.driver.memory', '16G')
              .config('spark.driver.maxResultSize', '10G')
              .config("spark.driver.bindAddress", "127.0.0.1")
              .getOrCreate()
      )
      spark.sparkContext.setLogLevel("WARN")
      return spark
  spark = get_spark()  

# COMMAND ----------

@F.udf(returnType=StringType())
def strip_path(s):
  import re
  match = re.search(r'[\/\=]*([\w\.-]+)\/*$', s)
  if match:
    return match.group(1)
  else:
    return 'none'


@F.udf(returnType=StringType())
def get_mac(file_name):
  import re
  try:
    return re.search(r'Logger_?([0-9a-f-]+)_', file_name).group(1)
  except:
    return ''
  

@F.udf(returnType=TimestampType())
def get_ts(file_name):
  from datetime import datetime
  import re
  try:
    ts = re.search(r'_(\d\d\d\d-\d\d-\d\d_\d\d\d\d\d\d)_', file_name).group(1)
    return datetime.strptime(ts, '%Y-%m-%d_%H%M%S')
  except:
    return None

# COMMAND ----------

def py_strip_path(s):
  import re
  match = re.search(r'[\/\=]*([\w\.-]+)\/*$', s)
  if match:
    return match.group(1)
  else:
    return 'none'
  

# COMMAND ----------

def files_from_path(path):
  #returns a dataframe with path, file_name, and size fields
  import pyspark.sql.functions as F
  df = sc.parallelize(dbutils.fs.ls(path)).toDF() \
          .withColumnRenamed('name','file_name') \
          .withColumn('file_name', strip_path(F.col('file_name'))) \
          .filter(~(F.col('file_name') == '_SUCCESS')) \
          .filter(~(F.col('file_name') == '_delta_log')) \
          .drop('modificationTime')
  return df
  
  
def files_from_list(l):
  #returns a dataframe with path and name fields
  import pyspark.sql.functions as F
  df = spark.createDataFrame(l, StringType()) \
            .withColumnRenamed('value', 'path') \
            .withColumn('file_name', strip_path(F.col('path')))
  return df

def files_from_table(project_name, data_year, db, table, field='file_name'):
  #returns a dataframe with argument-named field
  df = spark.sql(f'SELECT DISTINCT {field} FROM {db}.{table} WHERE project_name = "{project_name}" AND data_year = {data_year}')
  return df

# COMMAND ----------

def write_to_parquet(df, path, partition, mode='append'):
  if partition is None:
    df.write.mode(mode).parquet(path)
  else:
    df.write.partitionBy(partition).mode(mode).parquet(path)
  return path


def write_to_delta(df, path, partition, mode='append'):
  if partition is None:
    df.write.format('delta').mode(mode).save(path)
  else:
    df.write.format('delta').partitionBy(partition).mode(mode).save(path)
  return path

# COMMAND ----------

def net_change(dfSrc, dfTgt):
  #returns dfSrc with rows matching dfTgt removed
  import pyspark.sql.functions as F
  try:
    df = dfSrc.join(dfTgt, 'file_name', 'leftanti') 
    return df
  except:
    return dfSrc

# COMMAND ----------

def batch(df, cnt):
  #returns dataframe truncated to specified size
  if cnt > 0:
    return df.sort('file_name').limit(cnt)
  else:
    return df.sort('file_name')


def file_name_filter(df, text_to_match):
  #returns dataframe with names filtered to match specified text
  import pyspark.sql.functions as F
  return df.filter(F.col('file_name').like('%' + text_to_match))


def size_filter(df, value):
  #returns dataframe with files smaller than value removed
  import pyspark.sql.functions as F
  return df.filter(F.col('size') > value)


def list_from_df(df, field):
  #returns the specified column as a list
  return df.select(field).distinct().rdd.flatMap(lambda x: x).collect()

# COMMAND ----------

def process_failed_files(upstream_list, downstream_list, df_origin, failure_mode, project_name, data_year):
  import pyspark.sql.functions as F
  from pyspark.sql.types import StringType
  up_list = [py_strip_path(i) for i in upstream_list]
  down_list = [py_strip_path(i) for i in downstream_list]
  diff_list = [i for i in up_list if i not in down_list]
  df = spark.createDataFrame(diff_list, StringType()) \
            .withColumnRenamed('value', 'file_name') \
            .join(df_origin, 'file_name', 'inner') \
            .drop(df_origin.file_name) \
            .withColumn('failure_type', F.lit(failure_mode)) \
            .withColumn('mac_address', get_mac(F.col('file_name'))) \
            .withColumn('log_ts', get_ts(F.col('file_name'))) \
            .withColumn('project_name', F.lit(project_name)) \
            .withColumn('data_year', F.lit(data_year)) \
            .withColumn('ingest_ts', F.current_timestamp()) \
            .withColumn('file_size', F.col('size')) \
            .select('project_name', 'data_year', 'file_name', 'mac_address', 'log_ts', 'file_size', 'failure_type', 'ingest_ts')
  return df

# COMMAND ----------

def get_mac_and_ts(df):
  import pyspark.sql.functions as F
  
  return df.withColumn('log_ts', F.coalesce(F.unix_timestamp(F.to_timestamp(get_ts(F.col('file_name')),'yyyy-MM-dd_HHmmss')),F.lit(0))) \
              .withColumn('mac', get_mac(F.col('file_name')))



def meta_join(df_files, df_meta, files_field='mac', meta_field='mac_address', files_ts='log_ts', meta_start='start_ts', meta_end='end_ts'):
  from datetime import datetime
  import pyspark.sql.functions as F
  df = df_files.withColumn('join_cond', F.col(files_field)) \
                .join(df_meta.withColumn('join_cond', F.col(meta_field)) \
                      .withColumn('startTS', F.unix_timestamp(F.to_timestamp(F.col(meta_start)))) \
                      .withColumn('endTS', F.unix_timestamp(F.to_timestamp(F.col(meta_end)))) \
                , 'join_cond', 'inner') \
                .filter('log_ts >= startTS') \
                .filter('log_ts < endTS or endTS is null') \
                .drop('join_cond', files_field, 'startTS', 'endTS', 'size', meta_start, meta_end)
  return df


# COMMAND ----------

def read_extracted_files(path, ftp):
  df = spark.read.parquet(path)
  return df.join(ftp.select('file_name', 'software_program'), df.file_name == ftp.file_name, 'inner').drop(ftp.file_name)

def read_parsed_files(path, ftp, keep_raw = False):
  df = spark.read.format('delta').load(path)
  if keep_raw:
    return df.join(ftp.select('file_name'), df.file_name == ftp.file_name, 'inner').drop(ftp.file_name)
  else:
    return df.join(ftp.select('file_name'), df.file_name == ftp.file_name, 'inner').drop(ftp.file_name).drop('raw_value', 'sa', 'da', 'net', 'software_version')

