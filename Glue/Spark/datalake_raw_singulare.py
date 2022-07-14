# -*- coding: utf-8 -*-
from pyspark.sql.functions import *
from datetime import date
from functools import partial
import datetime
from datetime import datetime, timedelta
import boto3
import json
from pyspark.sql.types import IntegerType, DoubleType, StringType, DataType, TimestampType, LongType
from pyspark.context import SparkContext
# from awsglue.context import GlueContext
# from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import SparkSession



aws_access_key_id="ASIAS34UIATUBVXIXYEL"
aws_secret_access_key="SOxTd79D6AfLXu6381sCBA68mxj8PxnHwDVCNjmp"
aws_session_token="IQoJb3JpZ2luX2VjECwaCXNhLWVhc3QtMSJGMEQCIE0D0nE9LWJ55ADwNMGF55bd8sgF7e5038KWwOIKxG1IAiAEIqUX7koTG+EaXbr5fNUQcbSKLc9Mkzu6tLvCDI7gPyqiAwhlEAEaDDE5NzM0MjUyODc0NCIMGudBtmiGFCvhtzinKv8Ch26EDlSccShVayM7yGe1J/oC2chTBeh/zvuIs+ZmyGqUC9g2lCWDkbR3/mFYgZwzcjgdZlD61/jpjoSbxh/MkxyanB8gAeayEAKfb02w28k4lMclwezn3POmkmIvTNASdzMdlhuvtb1tF/GyNzLzSzGNZJDgF0psG4jXutFapi0rb6pfSr6RnUl5xchrby6HDEfel9EVHqKmgvXASVWCEoBv++WSNGzPMa8lzLPD72Adhch981My2N7k3yUirx5uQUQxKG4KwtHKmLT0hzhO9BU/H5hli2CYRwjyF1ZwY9zUt87UjSBqndR58JL2p4zLsG7J+roiJPz+u4VIEhizNZogWIoWFsRRTYjDogOUayUOr6Sa96GtMM3rX/03bivf26GpISFTEmhWd6Dd3ixebjjDTmqfsDBAPZvatF2ckabNiPONGAMFLdu5TmfxWpW4sqEIyqmotRXFc8PefDTMhrJMxLwEUXt3/WIV3XISsMXU3C59/S18Glsf96BPOMwwyqi3lgY6pwEJv2gR/HVPiSM14cnq8MocTK2uzkdK2ksn6gduf9KxtTVVMIuWM6VWBu/5+Ime/CXxm5H4v9imvzz/dS0j+uPiHmXbJ4v9ixpGcilNopqjURStp462OcgsNKaxPqoehejJgAF8nw7Nxa8P8ncP/4z5Ws3fuiZPQovSCNIuwtTYlI9SJdcOOHOiLW9hnA83D/ji2lDQN4pYuBi3QrEpGuNfhf3ck4nBIA=="

s3_client = boto3.client(
    "s3",
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    aws_session_token=aws_session_token
)
glue_client = boto3.client(
    "glue",
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    aws_session_token=aws_session_token,
    region_name='sa-east-1'
)

args = dict(
    domain='singulare',
    technology='sftp',
    file='liquidacao',
    source_bucket='captalys-analytics-land-production',
    date='2022-07-04'
)


# args = getResolvedOptions(sys.argv, ['technology', 'source_bucket', 'domain', 'destination_bucket', 'date', 'file'])


def get_table_schema(args, client):
    schema_info = {}
    schema_info['col'] = []
    schema_info['pk'] = []

    bucket = args["source_bucket"]
    file = f'{get_file_key(args, "schema")}.json'
    print(f"file : {file}")
    result = client.get_object(Bucket=bucket, Key=file)
    text = result["Body"].read().decode()
    schema = json.loads(text)

    for f in schema:
        schema_info['col'].append(f['column_name'])
        if f['primary_key']:
            schema_info['pk'].append(f['column_name'])


def get_file_key(glue_args, conf):
    path_key = f"{glue_args['domain']}/{conf}/{glue_args['technology']}/v1/json/lake/{glue_args['file']}"
    return path_key


def get_column_types_df(df):
    lst_column_types = []
    for col in df.dtypes:
        lst_column_types.append({'col': col[0], 'type': col[1]})
    return lst_column_types


def cast_dataframe_schema(args, client, dfin):
    bucket = args["source_bucket"]
    file = f'{get_file_key(args, "schema")}.json'
    try:
        result = client.get_object(Bucket=bucket, Key=file)
    except Exception as e:
        print(f"Error reading key {file} from bucket {bucket}: {e}")
    else:
        text = result["Body"].read().decode()
        schema = json.loads(text)

    df = dfin.select([when(col(c) == "", None).otherwise(col(c)).alias(c) for c in dfin.columns])

    for column in schema:
        fieldname = column['column_name']
        fieldtype = column['data_type']

        if 'int' in fieldtype:
            df = df.withColumn(fieldname, col(fieldname).cast(IntegerType()))
        elif 'serial' in fieldtype or 'bigint' in fieldtype or 'long' in fieldtype:
            df = df.withColumn(fieldname, col(fieldname).cast(LongType()))
        elif 'double' in fieldtype or 'real' in fieldtype or 'money' in fieldtype \
                or 'currency' in fieldtype or 'numeric' in fieldtype \
                or 'decimal' in fieldtype or 'float' in fieldtype:
            df = df.withColumn(fieldname, regexp_replace(col(fieldname), ",", '.')) \
                .withColumn(fieldname, col(fieldname).cast(DoubleType()))
        elif 'sys_commit_time' in fieldname:
            df = df.withColumn(fieldname, col(fieldname).cast(TimestampType()))
        elif 'text' in fieldtype:
            # df = df.withColumn(fieldname, udf_text_value(col(fieldname)))
            df = df.withColumn(fieldname, col(fieldname).cast(StringType())) \
                .withColumn(fieldname, regexp_replace(col(fieldname), "[\\r\\n]", '')) \
                .withColumn(fieldname, regexp_replace(col(fieldname), "[|]", '/')) \
                .withColumn(fieldname, trim(col(fieldname))) \
                .withColumn(fieldname, substring(col(fieldname), 1, 15000))
    return df


def delete_files(bucket_name, prefix):
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket_name)
    for obj in bucket.objects.filter(Prefix=prefix):
        s3.Object(bucket.name, obj.key).delete()


def get_contents_by_prefix(bucket_name, prefix_search):
    response = s3_client.list_objects_v2(
        Bucket=bucket_name,
        Prefix=prefix_search
    )

    if 'Contents' in response and isinstance(response['Contents'], list) and len(response['Contents']) > 0:
        return response['Contents']
    else:
        return []


def load_raw():
    client = boto3.client('s3')
    sparkContext = SparkContext()
    glueContext = GlueContext(sparkContext)
    spark = glueContext.spark_session

    date_path = args["date"].replace("-", "/")

    print('Iniciando Load dos arquivos na Land')

    s3_path_source = f's3a://{args["source_bucket"]}/{args["domain"]}/data/{args["technology"]}/v1/csv/lake/{args["file"]}/{date_path}'
    # s3_path_source = 's3://captalys-analytics-land-production/singulare/data/sftp/v1/csv/lake/estoque/2022/07/04/'

    print(s3_path_source)

    df_land = (
        spark
        .read
        .format("csv")
        .option("header", True)
        .option("inferSchema", True)
        .option("sep", ";")
        .option("encoding", "cp1252")
        .load(f'{s3_path_source}')
    )

    # dynamic_frame_from_s3 = DynamicFrame.fromDF(df_land, glueContext, 'dynamic_frame_from_s3')
    df_land = df_land.column(' NOME_FUNDO', 'NOME_FUNDO')

    print('Verifica se existe arquivo')
    if df_land and df_land.count() > 0:
        print(f'Existe arquivos, sendo processado para o arquivo: {args["file"]}')

        # data_frame_raw_s3 = dynamic_frame_from_s3.toDF()
        data_frame_raw = cast_dataframe_schema(args, client, df_land)
        print("Schema do dataframe convertido.")

        s3_path_target = f'{args["domain"]}/data/{args["technology"]}/v1/parquet/lake/singulare_{args["file"]}_diario/sys_file_date={args["date"]}'

        print('Verifica se existe arquivo no bucket rawzone, e excluí para não ter arquivo duplicado.')
        print(f'{args["destination_bucket"]}/{s3_path_target}')

        #delete_files(args["destination_bucket"], s3_path_target)

        print('Inicio - Escrevendo os arquivos no bucket Raw')
        dyf_raw = DynamicFrame.fromDF(data_frame_raw, glueContext, 'dyf_raw')
        dyf_raw.printSchema()
        dyf_raw.show(10)
        # glueContext.write_dynamic_frame.from_options(
        #     frame=dyf_raw,
        #     connection_type='s3',
        #     connection_options={"path": f's3://{args["destination_bucket"]}/{s3_path_target}'},
        #     format="parquet"
        # )
        print('Processado com sucesso. ')

    else:
        print('Não existe dataframe')


if __name__ == '__main__':
    load_raw()
