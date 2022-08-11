from src.input_output_functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as F
import pytest


def test1_py_strip_path():
    s = 'Logger00-0b-ab-83-27-fe_2020-06-10_154808_00005_can.asc'
    match = py_strip_path(s)
    assert match == 'Logger00-0b-ab-83-27-fe_2020-06-10_154808_00005_can.asc'


def test2_py_strip_path():
    s = 'filename=Logger00-0b-ab-83-27-fe_2020-06-10_154808_00005_can.asc/'
    match = py_strip_path(s)
    assert match == 'Logger00-0b-ab-83-27-fe_2020-06-10_154808_00005_can.asc'


def test3_py_strip_path():
    s = 'dbfs:/mnt/edl/raw/eaw_crop_harvest/gch/2020/Logger00-0b-ab-83-27-fe_2020-06-10_224224_00011_can.asc'
    match = py_strip_path(s)
    assert match == 'Logger00-0b-ab-83-27-fe_2020-06-10_224224_00011_can.asc'


def test4_py_strip_path():
    s = 'Logger00-0b-ab-83-27-fe_2020-06-10_154808_00005_can.asc.#'
    match = py_strip_path(s)
    assert match == 'none'


# def test1_files_from_path():
#    path = ''
#    match = files_from_path(path)
#    assert match == ''


def test1_files_from_list():
    file_list = ['dbfs:/mnt/edl/raw/eaw_crop_harvest/gch/2020/Logger00-0b-ab-83-27-fe_2020-06-10_215100_00009_can.asc',
                 'dbfs:/mnt/edl/raw/eaw_crop_harvest/gch/2020/Logger00-0b-ab-83-27-fe_2020-06-10_215555_00010_can.asc',
                 'dbfs:/mnt/edl/raw/eaw_crop_harvest/gch/2020/Logger00-0b-ab-83-27-fe_2020-06-10_224224_00011_can.asc']

    data = [('dbfs:/mnt/edl/raw/eaw_crop_harvest/gch/2020/Logger00-0b-ab-83-27-fe_2020-06-10_215100_00009_can.asc',
             'Logger00-0b-ab-83-27-fe_2020-06-10_215100_00009_can.asc'),
            ('dbfs:/mnt/edl/raw/eaw_crop_harvest/gch/2020/Logger00-0b-ab-83-27-fe_2020-06-10_215555_00010_can.asc',
             'Logger00-0b-ab-83-27-fe_2020-06-10_215555_00010_can.asc'),
            ('dbfs:/mnt/edl/raw/eaw_crop_harvest/gch/2020/Logger00-0b-ab-83-27-fe_2020-06-10_224224_00011_can.asc',
             'Logger00-0b-ab-83-27-fe_2020-06-10_224224_00011_can.asc')]

    schema = StructType([
        StructField('path', StringType(), False),
        StructField('file_name', StringType(), False)
    ])

    file_df = spark.createDataFrame(data, schema=schema)
    match_df = files_from_list(file_list)
    assert file_df.collect() == match_df.collect()


def test1_net_change():
    srcData = [('dbfs:/mnt/edl/raw/eaw_crop_harvest/gch/2020/Logger00-0b-ab-83-27-fe_2020-06-10_154907_00006_can.asc',
                'Logger00-0b-ab-83-27-fe_2020-06-10_154907_00006_can.asc', 5340648),
               ('dbfs:/mnt/edl/raw/eaw_crop_harvest/gch/2020/Logger00-0b-ab-83-27-fe_2020-06-10_162100_00007_can.asc',
                'Logger00-0b-ab-83-27-fe_2020-06-10_162100_00007_can.asc', 67303231),
               ('dbfs:/mnt/edl/raw/eaw_crop_harvest/gch/2020/Logger00-0b-ab-83-27-fe_2020-06-10_162950_00008_can.asc',
                'Logger00-0b-ab-83-27-fe_2020-06-10_162950_00008_can.asc', 27833695),
               ('dbfs:/mnt/edl/raw/eaw_crop_harvest/gch/2020/Logger00-0b-ab-83-27-fe_2020-06-10_215100_00009_can.asc',
                'Logger00-0b-ab-83-27-fe_2020-06-10_215100_00009_can.asc', 69632)]

    tgtData = [('dbfs:/mnt/sandbox/AWS-EDL-DATABRICKS-PROD-EAW/gch/2020/can/extracted/_SUCCESS', '_SUCCESS', 0),
               (
               'dbfs:/mnt/sandbox/AWS-EDL-DATABRICKS-PROD-EAW/gch/2020/can/extracted/filename=Logger00-0b-ab-83-27-fe_2020-06-10_154907_00006_can.asc/',
               'Logger00-0b-ab-83-27-fe_2020-06-10_154907_00006_can.asc', 0),
               (
               'dbfs:/mnt/sandbox/AWS-EDL-DATABRICKS-PROD-EAW/gch/2020/can/extracted/filename=Logger00-0b-ab-83-27-fe_2020-06-10_162100_00007_can.asc/',
               'Logger00-0b-ab-83-27-fe_2020-06-10_162100_00007_can.asc', 0),
               (
               'dbfs:/mnt/sandbox/AWS-EDL-DATABRICKS-PROD-EAW/gch/2020/can/extracted/filename=Logger00-0b-ab-83-27-fe_2020-06-11_181536_00023_can.asc/',
               'Logger00-0b-ab-83-27-fe_2020-06-11_181536_00023_can.asc', 0)]

    expData = [('dbfs:/mnt/edl/raw/eaw_crop_harvest/gch/2020/Logger00-0b-ab-83-27-fe_2020-06-10_162950_00008_can.asc',
                'Logger00-0b-ab-83-27-fe_2020-06-10_162950_00008_can.asc', 27833695),
               ('dbfs:/mnt/edl/raw/eaw_crop_harvest/gch/2020/Logger00-0b-ab-83-27-fe_2020-06-10_215100_00009_can.asc',
                'Logger00-0b-ab-83-27-fe_2020-06-10_215100_00009_can.asc', 69632)]

    schema = StructType([
        StructField('path', StringType(), False),
        StructField('file_name', StringType(), False),
        StructField('size', IntegerType(), False)
    ])

    src_df = spark.createDataFrame(srcData, schema=schema)
    tgt_df = spark.createDataFrame(tgtData, schema=schema)
    exp_df = spark.createDataFrame(expData, schema=schema)

    nc_df = net_change(src_df, tgt_df).select('path', 'file_name', 'size')
    assert nc_df.sort('file_name').collect() == exp_df.sort('file_name').collect()