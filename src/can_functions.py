# Databricks notebook source
from pyspark.sql import SparkSession
import pyspark.sql.functions as F 
from pyspark.sql.types import *
import os

if "DATABRICKS_RUNTIME_VERSION" not in os.environ:
  
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

# DBTITLE 1,UDF Definitions
@F.udf(returnType=StringType())
def strip_path(s):
  import re
  match = re.search(r'[\/\=]*([\w\.-]+)\/*$', s)
  print(match)
  if match:
    return match.group(1)
  else:
    return 'none'
  
mSchema = StructType([StructField('rts', StringType(), False),
                      StructField('net', StringType(), False),
                      StructField('prty', StringType(), False),
                      StructField('pgn', StringType(), False),
                      StructField('sa', StringType(), False),
                      StructField('dir', StringType(), False),
                      StructField('dlc', StringType(), False),
                      StructField('b1', StringType(), False),
                      StructField('b2', StringType(), False),
                      StructField('b3', StringType(), False),
                      StructField('b4', StringType(), False),
                      StructField('b5', StringType(), False),
                      StructField('b6', StringType(), False),
                      StructField('b7', StringType(), False),
                      StructField('b8', StringType(), False),])

  
@F.udf(returnType=mSchema)  
def split_message(message):
  import re
  split = re.search(r'(\d+\.\d+)\s+(\d+)\s+(\w\w)(\w\w\w\w)(\w\w)x\s+([RT])x\s+\w\s+(\d)\s*([0-9a-fA-F\s]*)', message)
  if not split:
    print(message)
    return['','','','','','','','', '', '', '', '', '', '', '']
  else:
    try:
      db = [str(int(x,16)) for x in split.group(8).split(' ')] + ['', '', '', '', '', '', '', ''] #padding for DLC < 8 to avoid index reference error
    except:
      db = ['', '', '', '', '', '', '', '']
    
    g3 = int(split.group(3),16)  #convert priority byte to decimal
    if g3%4 == 0:
      pgn = split.group(4)
    else:
      pgn = str(g3%4) + split.group(4)   #append non-zero dp/edp to front of pgn
    p = str(g3//4)   #three-bit priority value
    
    re =  [split.group(1), split.group(2), p, pgn, split.group(5), split.group(6), split.group(7), db[0], db[1], db[2], db[3], db[4], db[5], db[6], db[7]]
  return re
  
@F.udf(returnType=StringType())  
def make_cmd_byte_1(b1):
  if b1 not in range(0, 256):
      return 'None'
  try:
    be = '0x' + ('0'+hex(b1)[2:])[-2:].upper() 
    return be
  except:
    return 'None'


@F.udf(returnType=StringType())    
def make_cmd_byte_2(b1,b2):
  if b1 not in range(0, 256):
      return 'None'
  if b2 not in range(0, 256):
      return 'None'
  try:
    b1b2 = '0x' + ('0'+hex(b1)[2:])[-2:].upper() + ('0'+hex(b2)[2:])[-2:].upper()
    return b1b2 
  except:
    return None

  
@F.udf(returnType=StringType())    
def make_cmd_byte_3(b1,b2,b3):
  if b1 not in range(0, 256):
      return 'None'
  if b2 not in range(0, 256):
      return 'None'
  if b3 not in range(0, 256):
      return 'None'
  try:
    b1b2b3 = '0x' + ('0'+hex(b1)[2:])[-2:].upper() + ('0'+hex(b2)[2:])[-2:].upper() + ('0'+hex(b3)[2:])[-2:].upper()
    return b1b2b3 
  except:
    return None

  
   
@F.udf(returnType=LongType())  
def make_timestamp(yr,mo,dy,hr,mi,sec,file_name):
  import re
  from datetime import datetime
  try:
    s = str(yr) + '-' + str(mo) + '-' + str(dy) + ' ' + str(hr) + ':' + str(mi) + ':' + str(sec)
    ts = datetime.strptime(s, '%Y-%m-%d %H:%M:%S').timestamp()
    return int(ts)
  except:
    file_time_stamp_regex = r'.*_(?P<start_time>[0-9]{4}-[0-9]{2}-[0-9]{2}_[0-9]{6}).*'
    try:
      start_time = re.compile(file_time_stamp_regex)
      m = start_time.match(file_name)
      timestamp = m.group('start_time')
      if m is not None:
        ts = datetime.strptime(timestamp, '%Y-%m-%d_%H%M%S').timestamp()
        return int(ts)
    except Exception as e:
      return None
    return None
    


# COMMAND ----------

# DBTITLE 1,Read CAN Data from ASC
def read_can_from_asc(path_list):
  import pyspark.sql.functions as F
  from pyspark.sql.types import LongType, IntegerType
  
  df = spark.read.text(path_list) \
        .withColumn('filepath', F.input_file_name()) \
        .withColumn('file_name', strip_path(F.col('filepath'))) \
        .drop('filepath') \
        .filter(~((F.col('value').like('date%')) | (F.col('value').like('base%')) | (F.col('value').like('internal%')) | (F.col('value').like('Begin%')))) \
        .withColumn('spl', split_message('value')) \
        .withColumn('rts', F.col('spl.rts')) \
        .filter('rts != ""') \
        .withColumn('rts', (F.col('rts')*1000000.0).cast('Long')) \
        .withColumn('net', F.col('spl.net')) \
        .withColumn('prty', F.col('spl.prty')) \
        .withColumn('pgn', F.col('spl.pgn')) \
        .withColumn('sa', F.col('spl.sa')) \
        .withColumn('dir', F.col('spl.dir')) \
        .withColumn('dlc', F.col('spl.dlc')) \
        .withColumn('b1', F.col('spl.b1').cast('Integer')) \
        .withColumn('b2', F.col('spl.b2').cast('Integer')) \
        .withColumn('b3', F.col('spl.b3').cast('Integer')) \
        .withColumn('b4', F.col('spl.b4').cast('Integer')) \
        .withColumn('b5', F.col('spl.b5').cast('Integer')) \
        .withColumn('b6', F.col('spl.b6').cast('Integer')) \
        .withColumn('b7', F.col('spl.b7').cast('Integer')) \
        .withColumn('b8', F.col('spl.b8').cast('Integer')) \
        .withColumn('sa', F.concat(F.lit('0x'),F.col('sa'))) \
        .withColumn('pgn', F.concat(F.lit('0x'),F.col('pgn'))) \
        .withColumn('cmd1', make_cmd_byte_1(F.col('b1'))) \
        .withColumn('cmd2', make_cmd_byte_2(F.col('b1'),F.col('b2'))) \
        .withColumn('cmd3', make_cmd_byte_3(F.col('b1'),F.col('b2'),F.col('b3'))) \
        .select('file_name','rts','net','prty','pgn','cmd1','cmd2','cmd3','sa','dir','dlc','b1','b2','b3','b4','b5','b6','b7','b8')
  return df

# COMMAND ----------

# DBTITLE 1,CAN Timestamp Adjustment
def ts_adj(df, pgn='0xFEE6', sa='1C'):
  import pyspark.sql.functions as F
  from pyspark.sql.types import IntegerType
  from pyspark.sql import Window

  
  df_find_diff = df.filter((df.pgn == '0xFEE6') & (df.sa == '0x1C')) \
              .withColumn('mod', F.col('b1')%4) \
              .withColumn('prev', F.lag('b1').over(Window().partitionBy("file_name").orderBy('rts'))) \
              .where('b1 <> prev' and 'mod = 0') \
              .withColumn('rn', F.row_number().over(Window().partitionBy("file_name").orderBy('rts'))) \
              .filter('rn = 1') \
              .withColumn('sec', (F.col('b1')/4).cast('Integer')) \
              .withColumn('min', F.col('b2')) \
              .withColumn('hour', F.col('b3')) \
              .withColumn('day', F.ceil(F.col('b5')/4).cast('Integer')) \
              .withColumn('month', F.col('b4')) \
              .withColumn('year', F.col('b6').cast('Integer')+1985) \
              .withColumn('ts', make_timestamp('year','month','day','hour','min','sec','file_name')) \
              .withColumn('diff', F.col('rts')-(F.col('ts')*1000000)) \
              .selectExpr('file_name as file','diff','ts')
  
  df_out = df.join(df_find_diff, df.file_name == df_find_diff.file, 'inner') \
              .withColumn('ats', F.col('rts')-F.col('diff')) \
              .select('file_name','ats','net','prty','pgn','cmd1','cmd2','cmd3','sa','dir','dlc','b1','b2','b3','b4','b5','b6','b7','b8')
  
  return df_out
  


# COMMAND ----------

# DBTITLE 1,Bus Alias Functions
@F.udf(returnType=StringType())
def get_default_alias(bus):
  l = ['','A','B','C','D','E','F','G','H','I','J','K','L','M','N','O','P','Q','R','S','T','U','V','W','X','Y','Z','AA','BB','CC','DD','EE','FF','GG','HH','II','JJ','KK','LL','MM','NN','OO','PP','QQ','RR','SS','TT','UU','VV','WW','XX','YY','ZZ','AAA','BBB','CCC','DDD','EEE','FFF','GGG','HHH','III','JJJ','KKK','LLL','MMM','NNN','OOO','PPP','QQQ','RRR','SSS','TTT','UUU','VVV','WWW','XXX','YYY','ZZZ','AAAA','BBBB','CCCC','DDDD','EEEE','FFFF','GGGG','HHHH','IIII','JJJJ','KKKK','LLLL','MMMM','NNNN','OOOO','PPPP','QQQQ','RRRR','SSSS','TTTT','UUUU','VVVV','WWWW','XXXX','YYYY','ZZZZ']
  return l[bus]


def bus_alias(df_can, df_bus, df_meta):
  df1 = df_can.withColumn('native_bus', F.col('net').cast(IntegerType())).join(df_meta.select('file_name','machine_vin','machine_build_version','log_ts'), df_can.file_name == df_meta.file_name, 'inner').drop(df_meta.file_name)

  df2 = df1.join(df_bus, [df1.machine_vin == df_bus.machine_vin, df1.machine_build_version == df_bus.machine_build_version, df1.log_ts >= df_bus.start_ts, df1.log_ts < df_bus.end_ts, df1.native_bus == df_bus.bus_number], 'left') \
    .drop(df_bus.project_name) \
    .drop(df_bus.machine_vin) \
    .drop(df_bus.machine_build_version) \
    .drop(df_bus.bus_number)
  
  df3 = df2.withColumn('default_alias', get_default_alias(F.col('native_bus'))) \
    .withColumn('alias', F.coalesce(F.col('bus_alias'),F.col('default_alias'))) \
    .select('ats','native_bus','alias','prty','pgn','cmd1','cmd2','cmd3','sa','dir','dlc','b1','b2','b3','b4','b5','b6','b7','b8','file_name','software_program','log_ts')
  
  return df3


# COMMAND ----------

# DBTITLE 1,UDFs for Parsing
@F.udf(returnType=LongType())
def convert(b1, b2, b3, b4, b5, b6, b7, b8, startbit, sizebits):
  if startbit is None:
    return None
  if sizebits is None:
    return None
  data = {'b1':b1, 'b2':b2, 'b3':b3, 'b4':b4, 'b5':b5, 'b6':b6, 'b7':b7, 'b8':b8}
  startbyte = int(startbit)//8 + 1
  if sizebits == 8:
    try:
      return data['b{0}'.format(str(startbyte))]
    except:
      return None
  elif sizebits == 16:
    try:
      return data['b{0}'.format(str(startbyte))] + 256 * data['b{0}'.format(str(startbyte + 1))]
    except:
      return None
  elif sizebits == 24:
    try:
      return data['b{0}'.format(str(startbyte))] + 256 * data['b{0}'.format(str(startbyte + 1))] + 65536 * data['b{0}'.format(str(startbyte + 2))]
    except:
      return None
  elif sizebits == 32:
    try:
      return data['b{0}'.format(str(startbyte))] + 256 * data['b{0}'.format(str(startbyte + 1))] + 65536 * data['b{0}'.format(str(startbyte + 2))] + 16777216 * data['b{0}'.format(str(startbyte + 3))]
    except:
      return None
  else:
    try:
      muxbit = int(startbit)%8
      end = 0 - muxbit
      begin = end - sizebits
      if end == 0:
        return int((('00000000' + bin(data['b{0}'.format(str(startbyte))])[2:])[-8:])[begin:],2)
      else:
        return int((('00000000' + bin(data['b{0}'.format(str(startbyte))])[2:])[-8:])[begin:end],2)
    except:
      return None

    
    
    
@F.udf(returnType=DoubleType())    
def scale(unscaled, offset, factor, signed):
  try:
    return (unscaled * factor) + offset
  except:
    return None


    
@F.udf(returnType=StringType())
def get_dest_address(pgn):
  if pgn[-4] == 'F':
    return None
  else:
    return '0x' + pgn[-2:]

# COMMAND ----------

# DBTITLE 1,Convert to Key/Value pairs
def get_can_values(dfCan, dfCanDb):
  
  dfStd = dfCan.join(dfCanDb.filter("std_cmd_bytes is null"), [dfCan.pgn == dfCanDb.wire_pgn, dfCan.software_program == dfCanDb.software_program], 'inner') \
          .filter('alias = bus_alias or bus_alias is null') \
          .filter('log_ts >= start_ts or start_ts is null') \
          .filter('log_ts < end_ts or end_ts is null') \
          .drop(dfCanDb.software_program) \
          .withColumn('raw_value', convert('b1','b2','b3','b4','b5','b6','b7','b8','start_bit','size_bits')) \
          .withColumn('value', scale('raw_value','offset','factor','signed'))

  df1cb = dfCan.join(dfCanDb.filter("std_cmd_bytes is not null"), [dfCan.pgn == dfCanDb.wire_pgn, dfCan.cmd1 == dfCanDb.wire_cmd_bytes, dfCan.software_program == dfCanDb.software_program], 'inner') \
          .filter('alias = bus_alias or bus_alias is null') \
          .filter('log_ts >= start_ts or start_ts is null') \
          .filter('log_ts < end_ts or end_ts is null') \
          .drop(dfCanDb.software_program) \
          .withColumn('raw_value', convert('b1','b2','b3','b4','b5','b6','b7','b8','start_bit','size_bits')) \
          .withColumn('value', scale('raw_value','offset','factor','signed'))

  df2cb = dfCan.join(dfCanDb.filter("std_cmd_bytes is not null"), [dfCan.pgn == dfCanDb.wire_pgn, dfCan.cmd2 == dfCanDb.wire_cmd_bytes, dfCan.software_program == dfCanDb.software_program], 'inner') \
          .filter('alias = bus_alias or bus_alias is null') \
          .filter('log_ts >= start_ts or start_ts is null') \
          .filter('log_ts < end_ts or end_ts is null') \
          .drop(dfCanDb.software_program) \
          .withColumn('raw_value', convert('b1','b2','b3','b4','b5','b6','b7','b8','start_bit','size_bits')) \
          .withColumn('value', scale('raw_value','offset','factor','signed'))

  df3cb = dfCan.join(dfCanDb.filter("std_cmd_bytes is not null"), [dfCan.pgn == dfCanDb.wire_pgn, dfCan.cmd3 == dfCanDb.wire_cmd_bytes, dfCan.software_program == dfCanDb.software_program], 'inner') \
          .filter('alias = bus_alias or bus_alias is null') \
          .filter('log_ts >= start_ts or start_ts is null') \
          .filter('log_ts < end_ts or end_ts is null') \
          .drop(dfCanDb.software_program) \
          .withColumn('raw_value', convert('b1','b2','b3','b4','b5','b6','b7','b8','start_bit','size_bits')) \
          .withColumn('value', scale('raw_value','offset','factor','signed'))

  #union results of value conversions together
  dfData = dfStd.unionByName(df1cb) \
      .unionByName(df2cb) \
      .unionByName(df3cb) \
      .withColumn('da', get_dest_address(F.col('pgn'))) \
      .select('file_name','ats','common_signal_name','downsample_mode','raw_value','value', 'native_bus', 'alias', 'sa', 'da', 'software_version') \
      .sort('file_name','ats','common_signal_name')
  return dfData

# COMMAND ----------

# DBTITLE 1,Time Zone Functions
@F.udf(returnType=StringType())
def get_tz(lon,lat):
  from timezonefinder import TimezoneFinder
  try:
    tf = TimezoneFinder()
    return tf.timezone_at(lng=lon, lat=lat)
  except:
    return None
  
  
def create_tz_df(df):
  from pyspark.sql import Window
  df_tz = df.filter((F.col('common_signal_name') == 'latitude') | (F.col('common_signal_name') == 'longitude')) \
            .filter(F.col('raw_value') != F.lit(4294967295)) \
            .select('file_name','ats','common_signal_name','value') \
            .withColumn('rn', F.row_number().over(Window.partitionBy('file_name','common_signal_name').orderBy(F.col('ats').asc()))) \
            .filter(F.col('rn') == 1) \
            .drop('ats','rn') \
            .groupBy('file_name').pivot('common_signal_name').sum('value') \
            .withColumn('tz', get_tz(F.col('longitude'), F.col('latitude'))) \
            .select('file_name', 'tz')
  return df_tz

# COMMAND ----------

# DBTITLE 1,Aggregation Functions
import pyspark.sql.functions as F
from pyspark.sql.types import DoubleType
import statistics

@F.pandas_udf(DoubleType(), F.PandasUDFType.GROUPED_AGG)
def get_median(x):
  try:
    return statistics.median(x)
  except:
    return None

@F.pandas_udf(DoubleType(), F.PandasUDFType.GROUPED_AGG)
def get_mode(x):
  try:
    return statistics.multimode(x)[-1]
  except:
    return None

@F.pandas_udf(DoubleType(), F.PandasUDFType.GROUPED_AGG)
def get_max(x):
  try:
    return max(x)
  except:
    return None


def add_column(df, colName):
  df = df.withColumn(colName, F.lit(None)) \
      .withColumn(colName, F.col(colName).cast('Double'))
  return df


def aggregate(df):
  import pyspark.sql.functions as F
  from pyspark.sql import Window
  #max (udf)
  dfMax = df.filter("downsample_mode = 'max'") \
      .filter("value is not null") \
      .groupBy('file_name','common_signal_name','sample').agg(get_max('value').alias('value'))
  #median (udf)
  dfMedian = df.filter("downsample_mode = 'median'") \
      .filter("value is not null") \
      .groupBy('file_name','common_signal_name','sample').agg(get_median('value').alias('value'))
  #mode (udf)
  dfMode = df.filter("downsample_mode = 'mode'") \
      .filter("value is not null") \
      .groupBy('file_name','common_signal_name','sample').agg(get_mode('value').alias('value'))
  #first non-null value
  dfFirst = df.filter("downsample_mode = 'first'") \
      .filter("value is not null") \
      .withColumn('idx', F.row_number().over(Window.partitionBy('file_name','common_signal_name','sample').orderBy(F.col('ats').asc()))) \
      .filter("idx = 1") \
      .select('file_name','common_signal_name','sample','value')
  #last non-null value
  dfLast = df.filter("downsample_mode = 'last'") \
      .filter("value is not null") \
      .withColumn('idx', F.row_number().over(Window.partitionBy('file_name','common_signal_name','sample').orderBy(F.col('ats').desc()))) \
      .filter("idx = 1") \
      .select('file_name','common_signal_name','sample','value')
  #use max for timestamp
  dfTimestamp = df.filter("downsample_mode = 'timestamp'") \
      .groupBy('file_name','common_signal_name','sample').max('value') \
      .withColumn('value',F.col('max(value)')).drop('max(value)')
  #consolidate aggregated dataframes
  dfAgg = dfMax \
      .unionByName(dfMedian) \
      .unionByName(dfMode) \
      .unionByName(dfFirst) \
      .unionByName(dfLast) \
      .unionByName(dfTimestamp)
  #pivot aggregated dataframe
  dfPvt = dfAgg.groupBy(['file_name','sample']) \
      .pivot('common_signal_name').sum('value') 
  return dfPvt


# COMMAND ----------

# DBTITLE 1,Downsample function
def get_sample_ids(df, freq):
    import pyspark.sql.functions as F
    from pyspark.sql.types import DecimalType, LongType
    from pyspark.sql import Window
    
    factor1 = 1000000 / float(int(freq))
    factor2 = 1 / float(int(freq))
    
    dfDownsample = df.withColumn('sample', (((F.col('ats') / F.lit(factor1)).cast('Long')) * F.lit(factor2)).cast('Decimal(12,1)')) \
                    .withColumn('min_sample', F.min('sample').over(Window.partitionBy('file_name'))) \
                    .filter(F.col('sample') != F.col('min_sample')) \
                    .drop('min_sample')
    
    return dfDownsample
  
def column_verify(df, col_list):
    pvtCols = df.columns
    for c in col_list:
      if c not in pvtCols:
        df = add_column(df, c)

    df = df.withColumn('time_stamp', F.col('sample')) \
            .select(['time_stamp','file_name'] + col_list)
            
    return df

# COMMAND ----------

# DBTITLE 1,Tall Table Transformation
@F.udf(returnType=StringType())
def get_utc_ts(t):
  from datetime import datetime
  return datetime.strftime(datetime.fromtimestamp(t/1000000), '%Y-%m-%d %H:%M:%S.%f')

@F.udf(returnType=StringType())
def get_local_ts(ts,tz):
  from datetime import datetime
  import pytz
  return datetime.strftime(ts.astimezone(pytz.timezone(tz)),'%Y-%m-%d %H:%M:%S.%f')



def transform(df, year):
  from datetime import datetime
  import pyspark.sql.functions as F
  import pyspark.sql.types as T
  ts = datetime.now()
  df_trans = df.withColumnRenamed('ats', 'utc_epoch') \
            .withColumnRenamed('tz', 'local_tz') \
            .withColumn('utc_ts', get_utc_ts(F.col('utc_epoch')).cast(T.TimestampType())) \
            .withColumn('local_ts', get_local_ts(F.col('utc_ts'), F.col('local_tz')).cast(T.TimestampType())) \
            .withColumn('data_year', F.lit(year)) \
            .withColumnRenamed('common_signal_name', 'metric_name') \
            .withColumn('metric_value_raw', F.col('raw_value').cast('double')) \
            .withColumnRenamed('value', 'metric_value') \
            .withColumnRenamed('sa', 'source_address') \
            .withColumnRenamed('da', 'destination_address') \
            .withColumn('bus_number', F.col('native_bus')) \
            .withColumn('ts_secs', (F.col('utc_epoch') / 1000000).cast('int')) \
            .withColumnRenamed('name', 'file_name') \
            .withColumnRenamed('mac_address', 'logger_mac_address') \
            .withColumn('metric_value_units', F.lit(None).cast(T.StringType())) \
            .withColumn('ingest_ts', F.lit(ts).cast('TIMESTAMP')) \
            .withColumn('bus_alias', F.col('alias')) \
            .select('project_name', 'data_year', 'machine_vin', 'machine_build_version', 'software_program', 'software_version', 
                    'file_name', 'utc_epoch', 'utc_ts', 'local_ts', 'local_tz', 'logger_mac_address', 'machine_name', 'machine_model', 
                    'metric_name', 'metric_value_raw', 'metric_value', 'source_address', 'destination_address', 'bus_number', 'bus_alias', 'ingest_ts')
  return df_trans
