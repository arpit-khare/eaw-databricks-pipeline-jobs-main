import pyspark.sql.functions as F
import pytest
from src.can_functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *


def test_read_can_from_asc():
    file_path = ['tests/data/Sample1_can.asc']

    mSchema = StructType([StructField('file_name', StringType(), False),
                          StructField('rts', LongType(), False),
                          StructField('net', StringType(), False),
                          StructField('prty', StringType(), False),
                          StructField('pgn', StringType(), False),
                          StructField('cmd1', StringType(), False),
                          StructField('cmd2', StringType(), False),
                          StructField('cmd3', StringType(), False),
                          StructField('sa', StringType(), False),
                          StructField('dir', StringType(), False),
                          StructField('dlc', StringType(), False),
                          StructField('b1', IntegerType(), False),
                          StructField('b2', IntegerType(), False),
                          StructField('b3', IntegerType(), False),
                          StructField('b4', IntegerType(), False),
                          StructField('b5', IntegerType(), False),
                          StructField('b6', IntegerType(), False),
                          StructField('b7', IntegerType(), False),
                          StructField('b8', IntegerType(), False)])

    data = [
        ('Sample1_can.asc', 0, '4', '1', '0xFFFF', '0xDF', '0xDF00', '0xDF0004', '0x7E', 'R', '8', 223, 0, 4, 0, 11, 0,
         84, 193),
        (
        'Sample1_can.asc', 2690, '20', '2', '0xFF18', '0xC0', '0xC017', '0xC01700', '0xEB', 'R', '8', 192, 23, 0, 2, 64,
        7, 2, 112),
        (
        'Sample1_can.asc', 12697, '20', '3', '0xFF18', '0xC0', '0xC009', '0xC00900', '0xEB', 'R', '8', 192, 9, 0, 3, 64,
        7, 3, 176),
        ('Sample1_can.asc', 13001, '20', '4', '0xF623', '0xFF', '0xFF01', '0xFF0140', '0xEB', 'R', '8', 255, 1, 64, 255,
         63, 255, 223, 240),
        ('Sample1_can.asc', 13265, '20', '5', '0xF624', '0xFF', '0xFF02', '0xFF0240', '0xEB', 'R', '8', 255, 2, 64, 255,
         63, 255, 223, 16),
        ('Sample1_can.asc', 13554, '20', '6', '0xF625', '0xFF', '0xFF05', '0xFF0540', '0xEB', 'R', '8', 255, 5, 64, 255,
         63, 255, 223, 208),
        ('Sample1_can.asc', 135808, '21', '7', '0xFEE6', '0xD1', '0xD129', '0xD12915', '0x1C', 'R', '8', 209, 41, 21, 6,
         40, 35, 255, 249),
        (
        'Sample1_can.asc', 333592, '21', '1', '0x1FEE6', '0xD2', '0xD229', '0xD22915', '0x1C', 'R', '8', 210, 41, 21, 6,
        40, 35, 255, 249),
        (
        'Sample1_can.asc', 533635, '21', '2', '0x1FEE6', '0xD3', '0xD329', '0xD32915', '0x1C', 'R', '8', 211, 41, 21, 6,
        40, 35, 255, 249),
        (
        'Sample1_can.asc', 733681, '21', '3', '0x1FEE6', '0xD4', '0xD429', '0xD42915', '0x1C', 'R', '8', 212, 41, 21, 6,
        40, 35, 255, 249),
        (
        'Sample1_can.asc', 933582, '21', '4', '0x1FEE6', '0xD4', '0xD429', '0xD42915', '0x1C', 'R', '8', 212, 41, 21, 6,
        40, 35, 255, 249),
        ('Sample1_can.asc', 1133551, '21', '5', '0x1FEE6', '0xD5', '0xD529', '0xD52915', '0x1C', 'R', '8', 213, 41, 21,
         6, 40, 35, 255, 249),
        ('Sample1_can.asc', 1333551, '21', '6', '0x1FEE6', '0xD6', '0xD629', '0xD62915', '0x1C', 'R', '8', 214, 41, 21,
         6, 40, 35, 255, 249),
        ('Sample1_can.asc', 1533551, '21', '7', '0x1FEE6', '0xD7', '0xD729', '0xD72915', '0x1C', 'R', '8', 215, 41, 21,
         6, 40, 35, 255, 249),
        ('Sample1_can.asc', 1733551, '21', '1', '0x2FEE6', '0xD8', '0xD829', '0xD82915', '0x1C', 'R', '8', 216, 41, 21,
         6, 40, 35, 255, 249),
        ('Sample1_can.asc', 1933551, '21', '2', '0x2FEE6', '0xD8', '0xD829', '0xD82915', '0x1C', 'R', '8', 216, 41, 21,
         6, 40, 35, 255, 249),
        ('Sample1_can.asc', 2133551, '21', '3', '0x3FEE6', '0xD9', '0xD929', '0xD92915', '0x1C', 'R', '8', 217, 41, 21,
         6, 40, 35, 255, 249)
    ]

    file_df = spark.createDataFrame(data, schema=mSchema)
    expected_df = read_can_from_asc(file_path)
    assert file_df.collect() == expected_df.collect()


def test_ts_adj():
    file_path = 'tests/data/Sample1_can.asc'

    oSchema = StructType([StructField('file_name', StringType(), False),
                          StructField('ats', LongType(), False),
                          StructField('net', StringType(), False),
                          StructField('prty', StringType(), False),
                          StructField('pgn', StringType(), False),
                          StructField('cmd1', StringType(), False),
                          StructField('cmd2', StringType(), False),
                          StructField('cmd3', StringType(), False),
                          StructField('sa', StringType(), False),
                          StructField('dir', StringType(), False),
                          StructField('dlc', StringType(), False),
                          StructField('b1', IntegerType(), False),
                          StructField('b2', IntegerType(), False),
                          StructField('b3', IntegerType(), False),
                          StructField('b4', IntegerType(), False),
                          StructField('b5', IntegerType(), False),
                          StructField('b6', IntegerType(), False),
                          StructField('b7', IntegerType(), False),
                          StructField('b8', IntegerType(), False), ])

    iSchema = StructType([StructField('file_name', StringType(), False),
                          StructField('rts', LongType(), False),
                          StructField('net', StringType(), False),
                          StructField('prty', StringType(), False),
                          StructField('pgn', StringType(), False),
                          StructField('cmd1', StringType(), False),
                          StructField('cmd2', StringType(), False),
                          StructField('cmd3', StringType(), False),
                          StructField('sa', StringType(), False),
                          StructField('dir', StringType(), False),
                          StructField('dlc', StringType(), False),
                          StructField('b1', IntegerType(), False),
                          StructField('b2', IntegerType(), False),
                          StructField('b3', IntegerType(), False),
                          StructField('b4', IntegerType(), False),
                          StructField('b5', IntegerType(), False),
                          StructField('b6', IntegerType(), False),
                          StructField('b7', IntegerType(), False),
                          StructField('b8', IntegerType(), False), ])

    output_data = [
        ('Sample1_can.asc', 1591825312266319, '4', '0C', '0xFFFF', '0xDF', '0xDF00', '0xDF0004', '0x7E', 'R', '8', 223,
         0, 4, 0, 11, 0, 84, 193),
        ('Sample1_can.asc', 1591825312269009, '20', '0C', '0xFF18', '0xC0', '0xC017', '0xC01700', '0xEB', 'R', '8', 192,
         23, 0, 2, 64, 7, 2, 112),
        ('Sample1_can.asc', 1591825312279016, '20', '0C', '0xFF18', '0xC0', '0xC009', '0xC00900', '0xEB', 'R', '8', 192,
         9, 0, 3, 64, 7, 3, 176),
        ('Sample1_can.asc', 1591825312279320, '20', '0C', '0xF623', '0xFF', '0xFF01', '0xFF0140', '0xEB', 'R', '8', 255,
         1, 64, 255, 63, 255, 223, 240),
        ('Sample1_can.asc', 1591825312279584, '20', '0C', '0xF624', '0xFF', '0xFF02', '0xFF0240', '0xEB', 'R', '8', 255,
         2, 64, 255, 63, 255, 223, 16),
        ('Sample1_can.asc', 1591825312279873, '20', '0C', '0xF625', '0xFF', '0xFF05', '0xFF0540', '0xEB', 'R', '8', 255,
         5, 64, 255, 63, 255, 223, 208),
        ('Sample1_can.asc', 1591825312402127, '21', '0C', '0xFEE6', '0xD1', '0xD129', '0xD12915', '0x1C', 'R', '8', 209,
         41, 21, 6, 40, 35, 255, 249),
        ('Sample1_can.asc', 1591825312599911, '21', '0C', '0xFEE6', '0xD2', '0xD229', '0xD22915', '0x1C', 'R', '8', 210,
         41, 21, 6, 40, 35, 255, 249),
        ('Sample1_can.asc', 1591825312799954, '21', '0C', '0xFEE6', '0xD3', '0xD329', '0xD32915', '0x1C', 'R', '8', 211,
         41, 21, 6, 40, 35, 255, 249),
        ('Sample1_can.asc', 1591825313000000, '21', '0C', '0xFEE6', '0xD4', '0xD429', '0xD42915', '0x1C', 'R', '8', 212,
         41, 21, 6, 40, 35, 255, 249),
        ('Sample1_can.asc', 1591825313199901, '21', '0C', '0xFEE6', '0xD4', '0xD429', '0xD42915', '0x1C', 'R', '8', 212,
         41, 21, 6, 40, 35, 255, 249),
        ('Sample1_can.asc', 1591825313399870, '21', '0C', '0xFEE6', '0xD5', '0xD529', '0xD52915', '0x1C', 'R', '8', 213,
         41, 21, 6, 40, 35, 255, 249),
        ('Sample1_can.asc', 1591825313599870, '21', '0C', '0xFEE6', '0xD6', '0xD629', '0xD62915', '0x1C', 'R', '8', 214,
         41, 21, 6, 40, 35, 255, 249),
        ('Sample1_can.asc', 1591825313799870, '21', '0C', '0xFEE6', '0xD7', '0xD729', '0xD72915', '0x1C', 'R', '8', 215,
         41, 21, 6, 40, 35, 255, 249),
        ('Sample1_can.asc', 1591825313999870, '21', '0C', '0xFEE6', '0xD8', '0xD829', '0xD82915', '0x1C', 'R', '8', 216,
         41, 21, 6, 40, 35, 255, 249),
        ('Sample1_can.asc', 1591825314199870, '21', '0C', '0xFEE6', '0xD8', '0xD829', '0xD82915', '0x1C', 'R', '8', 216,
         41, 21, 6, 40, 35, 255, 249),
        ('Sample1_can.asc', 1591825314399870, '21', '0C', '0xFEE6', '0xD9', '0xD929', '0xD92915', '0x1C', 'R', '8', 217,
         41, 21, 6, 40, 35, 255, 249)

    ]

    data = [
        ('Sample1_can.asc', 0, '4', '0C', '0xFFFF', '0xDF', '0xDF00', '0xDF0004', '0x7E', 'R', '8', 223, 0, 4, 0, 11, 0,
         84, 193),
        ('Sample1_can.asc', 2690, '20', '0C', '0xFF18', '0xC0', '0xC017', '0xC01700', '0xEB', 'R', '8', 192, 23, 0, 2,
         64, 7, 2, 112),
        ('Sample1_can.asc', 12697, '20', '0C', '0xFF18', '0xC0', '0xC009', '0xC00900', '0xEB', 'R', '8', 192, 9, 0, 3,
         64, 7, 3, 176),
        (
        'Sample1_can.asc', 13001, '20', '0C', '0xF623', '0xFF', '0xFF01', '0xFF0140', '0xEB', 'R', '8', 255, 1, 64, 255,
        63, 255, 223, 240),
        (
        'Sample1_can.asc', 13265, '20', '0C', '0xF624', '0xFF', '0xFF02', '0xFF0240', '0xEB', 'R', '8', 255, 2, 64, 255,
        63, 255, 223, 16),
        (
        'Sample1_can.asc', 13554, '20', '0C', '0xF625', '0xFF', '0xFF05', '0xFF0540', '0xEB', 'R', '8', 255, 5, 64, 255,
        63, 255, 223, 208),
        (
        'Sample1_can.asc', 135808, '21', '0C', '0xFEE6', '0xD1', '0xD129', '0xD12915', '0x1C', 'R', '8', 209, 41, 21, 6,
        40, 35, 255, 249),
        (
        'Sample1_can.asc', 333592, '21', '0C', '0xFEE6', '0xD2', '0xD229', '0xD22915', '0x1C', 'R', '8', 210, 41, 21, 6,
        40, 35, 255, 249),
        (
        'Sample1_can.asc', 533635, '21', '0C', '0xFEE6', '0xD3', '0xD329', '0xD32915', '0x1C', 'R', '8', 211, 41, 21, 6,
        40, 35, 255, 249),
        (
        'Sample1_can.asc', 733681, '21', '0C', '0xFEE6', '0xD4', '0xD429', '0xD42915', '0x1C', 'R', '8', 212, 41, 21, 6,
        40, 35, 255, 249),
        (
        'Sample1_can.asc', 933582, '21', '0C', '0xFEE6', '0xD4', '0xD429', '0xD42915', '0x1C', 'R', '8', 212, 41, 21, 6,
        40, 35, 255, 249),
        ('Sample1_can.asc', 1133551, '21', '0C', '0xFEE6', '0xD5', '0xD529', '0xD52915', '0x1C', 'R', '8', 213, 41, 21,
         6, 40, 35, 255, 249),
        ('Sample1_can.asc', 1333551, '21', '0C', '0xFEE6', '0xD6', '0xD629', '0xD62915', '0x1C', 'R', '8', 214, 41, 21,
         6, 40, 35, 255, 249),
        ('Sample1_can.asc', 1533551, '21', '0C', '0xFEE6', '0xD7', '0xD729', '0xD72915', '0x1C', 'R', '8', 215, 41, 21,
         6, 40, 35, 255, 249),
        ('Sample1_can.asc', 1733551, '21', '0C', '0xFEE6', '0xD8', '0xD829', '0xD82915', '0x1C', 'R', '8', 216, 41, 21,
         6, 40, 35, 255, 249),
        ('Sample1_can.asc', 1933551, '21', '0C', '0xFEE6', '0xD8', '0xD829', '0xD82915', '0x1C', 'R', '8', 216, 41, 21,
         6, 40, 35, 255, 249),
        ('Sample1_can.asc', 2133551, '21', '0C', '0xFEE6', '0xD9', '0xD929', '0xD92915', '0x1C', 'R', '8', 217, 41, 21,
         6, 40, 35, 255, 249)
    ]

    input_df = spark.createDataFrame(data, schema=iSchema)

    expected_df = spark.createDataFrame(output_data, schema=oSchema)

    output_df = ts_adj(input_df)
    assert output_df.collect() == expected_df.collect()


def test_bus_alias():
    canSchema = StructType([
        StructField('ats', StringType(), False),
        StructField('net', StringType(), False),
        StructField('prty', StringType(), False),
        StructField('pgn', StringType(), False),
        StructField('cmd1', StringType(), False),
        StructField('cmd2', StringType(), False),
        StructField('cmd3', StringType(), False),
        StructField('sa', StringType(), False),
        StructField('dir', StringType(), False),
        StructField('dlc', StringType(), False),
        StructField('b1', IntegerType(), False),
        StructField('b2', IntegerType(), False),
        StructField('b3', IntegerType(), False),
        StructField('b4', IntegerType(), False),
        StructField('b5', IntegerType(), False),
        StructField('b6', IntegerType(), False),
        StructField('b7', IntegerType(), False),
        StructField('b8', IntegerType(), False),
        StructField('file_name', StringType(), False),
        StructField('software_program', StringType(), False)
    ])

    canData = [
        ('1592214229314253', '3', '18', '0xFFFE', '0x3A', '0x3AFF', '0x3AFF0F', '0xF3', 'R', '8', 58, 255, 15, 255, 255,
         255, 255, 255, 'file1', 'Edison'),
        (
        '1592214229315303', '2', '0C', '0xF004', '0xF1', '0xF1FF', '0xF1FFA3', '0x96', 'R', '8', 241, 255, 163, 178, 68,
        255, 255, 255, 'file1', 'Edison'),
        ('1592214229325564', '2', '10', '0xFFFF', '0x09', '0x09F5', '0x09F5F3', '0x96', 'R', '8', 9, 245, 243, 255, 255,
         255, 255, 255, 'file2', 'Edison'),
        ('1592214229327590', '1', '04', '0xEF00', '0x64', '0x6415', '0x641514', '0x06', 'R', '8', 100, 21, 20, 240, 230,
         46, 96, 34, 'file2', 'Edison')
    ]

    busListSchema = StructType([
        StructField('project_name', StringType(), False),
        StructField('machine_vin', StringType(), False),
        StructField('machine_build_version', StringType(), False),
        StructField('bus_number', IntegerType(), False),
        StructField('bus_alias', StringType(), False),
        StructField('start_ts', StringType(), False),
        StructField('end_ts', LongType(), True),
        StructField('comments', LongType(), True),
        StructField('ingest_ts', StringType(), False)
    ])

    busListData = [
        ('gch', '1H0S770SKLT815040', 'v1', 3, 'A', 1546322400, 1577858399, None, '2022-03-30 21:25:47')
    ]

    metaSchema = StructType([
        StructField('file_name', StringType(), False),
        StructField('log_ts', LongType(), False),
        StructField('project_name', StringType(), False),
        StructField('machine_vin', StringType(), False),
        StructField('machine_build_version', StringType(), False),
        StructField('software_program', StringType(), False)
    ])

    metaData = [
        ('file1', 1557044567, 'gch', '1H0S770SKLT815040', 'v1', 'Edison'),
        ('file2', 1588666967, 'gch', '1H0S770SKLT815040', 'v1', 'Edison')
    ]

    expSchema = StructType([
        StructField('ats', StringType(), False),
        StructField('native_bus', IntegerType(), False),
        StructField('alias', StringType(), False),
        StructField('prty', StringType(), False),
        StructField('pgn', StringType(), False),
        StructField('cmd1', StringType(), False),
        StructField('cmd2', StringType(), False),
        StructField('cmd3', StringType(), False),
        StructField('sa', StringType(), False),
        StructField('dir', StringType(), False),
        StructField('dlc', StringType(), False),
        StructField('b1', IntegerType(), False),
        StructField('b2', IntegerType(), False),
        StructField('b3', IntegerType(), False),
        StructField('b4', IntegerType(), False),
        StructField('b5', IntegerType(), False),
        StructField('b6', IntegerType(), False),
        StructField('b7', IntegerType(), False),
        StructField('b8', IntegerType(), False),
        StructField('file_name', StringType(), False),
        StructField('software_program', StringType(), False),
        StructField('log_ts', LongType(), False)
    ])

    expData = [
        ('1592214229314253', 3, 'A', '18', '0xFFFE', '0x3A', '0x3AFF', '0x3AFF0F', '0xF3', 'R', '8', 58, 255, 15, 255,
         255, 255, 255, 255, 'file1', 'Edison', 1557044567),
        ('1592214229315303', 2, 'B', '0C', '0xF004', '0xF1', '0xF1FF', '0xF1FFA3', '0x96', 'R', '8', 241, 255, 163, 178,
         68, 255, 255, 255, 'file1', 'Edison', 1557044567),
        ('1592214229325564', 2, 'B', '10', '0xFFFF', '0x09', '0x09F5', '0x09F5F3', '0x96', 'R', '8', 9, 245, 243, 255,
         255, 255, 255, 255, 'file2', 'Edison', 1588666967),
        ('1592214229327590', 1, 'A', '04', '0xEF00', '0x64', '0x6415', '0x641514', '0x06', 'R', '8', 100, 21, 20, 240,
         230, 46, 96, 34, 'file2', 'Edison', 1588666967)
    ]

    df_can = spark.createDataFrame(canData, schema=canSchema)
    df_bus_list = spark.createDataFrame(busListData, schema=busListSchema)
    df_meta = spark.createDataFrame(metaData, schema=metaSchema)

    df_exp = spark.createDataFrame(expData, schema=expSchema)

    df_actual = bus_alias(df_can, df_bus_list, df_meta)

    assert df_actual.sort('ats').collect() == df_exp.sort('ats').collect()