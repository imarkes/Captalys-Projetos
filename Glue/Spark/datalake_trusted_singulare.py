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
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import SparkSession
from pyspark.sql import functions as f

aws_access_key_id = "ASIAS34UIATUD5SYQNXR"
aws_secret_access_key = "NFwxMDU45kPHTp5KYeVPPDxcVloNciVz3Ufyom2F"
aws_session_token = "IQoJb3JpZ2luX2VjEID//////////wEaCXNhLWVhc3QtMSJHMEUCIQCc4DtuwHQMqUOtAzWFYvu00KXxmjuSPF8k9wy+WZ9uowIgZH0Y9ikMKhtjSYGX7+VQ50URbNyNiF5M1w2eNDzXPrwqqwMIqf//////////ARABGgwxOTczNDI1Mjg3NDQiDFHrR/dwI3Q6DqQErCr/AptQYMZpaEY/19c955YITYJa/0P2ILfZF/aKis3MrdE+MSTG0mx35zgJ+bKQdP/5omVXg40kox3YyG7CV+n7pOCvsf8B9F/yXf6DMZ5xAPjw5uqy3MUXN55gBDXucKn9DDNdNpunQUKAQa7wnjNt1cuaSQ0kHskwGrhgooRiYtXz7UNvSTDx4KbMyLZDN6O3irdq5Xx2pTc/LNMaK8SePFYpz+ku/eSgG/tcuFar84f0tMiqJGJ3yJAwq0sH+t9LmSBNs+l7yeQgRA+c9RT0fuTzlOHPQtb3JEBTAupt06XzHWeLq+6CVgndJmqfA86dFTDj6D9Jg+poXtPYtVl9h5Vev/KbQOjRVXC0esGXC2lr+8sthZuxq404uLpTzULukmKgUeVg+cQq4+WfUX4sTsSOFH/nm9i6ViE6rq/1KbRpfsyTF/49b/ofFEKAIX9m7oMqaD5MVLDzq3djxjl0SCYG22LCAgrGLgw0C8/ym9VwWF4xUqU7uzsCSox4vfSLMNCvkZYGOqYBp9YavVUWiupqeUddSsDame/pfjiALVpghSlOfweRsgvHYOcmu+6ZawI1ar11Vmyci5fGf+kNxnCPH8K2xO3FcZQqy2WXDyVoaYT8BpucC9URRGs5SZokdnedV9vzND8JOWAo9lOrikyeXuDdjiQha6r7mwC0PkumB3t7484MeSkLfZPLjyx2xVLV+1BiuNJ5wdZ+SSs2eFW5FzVQ5kJZ9xmcI+AgPA=="
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
    file='estoque',
    source_bucket='captalys-analytics-land-production',
    date='2022-02-25'
)

def get_connection(conn_name):
    client = boto3.client('glue')
    response = client.get_connection(Name=conn_name)

    # https://docs.aws.amazon.com/glue/latest/dg/connection-defining.html#connection-properties-jdbc
    jdbc_url = response['Connection']['ConnectionProperties']['JDBC_CONNECTION_URL'].split('/')
    database_port = jdbc_url[2].split(':')

    conn_param = {}
    conn_param['url'] = response['Connection']['ConnectionProperties']['JDBC_CONNECTION_URL']
    conn_param['database'] = jdbc_url[3]
    conn_param['hostname'] = database_port[0]
    conn_param['port'] = database_port[1]
    conn_param['user'] = response['Connection']['ConnectionProperties']['USERNAME']
    conn_param['password'] = response['Connection']['ConnectionProperties']['PASSWORD']

    return conn_param

def get_table_schema(args, client):
    schema_info = {}
    schema_info['col'] = []
    schema_info['pk'] = []

    bucket = args["land_bucket"]
    file = f'{get_file_key(args, "schema")}.json'
    print(f"file : {file}")
    result = client.get_object(Bucket=bucket, Key=file)
    text = result["Body"].read().decode()
    schema = json.loads(text)

    for f in schema:
        schema_info['col'].append(f['column_name'])

        if f['primary_key']:
            schema_info['pk'].append(f['column_name'])

    schema_info['col'].append('sys_commit_time')
    schema_info['col'].append('sys_file_date')
    schema_info['col'].append('sys_file_name')
    schema_info['col'].append('sys_operation')
    schema_info['col'].append('sys_commit_timestamp')

    return schema_info

def get_file_key(glue_args, conf):
    path_key = f"{glue_args['domain']}/{conf}/{glue_args['technology']}/v1/json/lake/{glue_args['file']}"
    # print(f"path_key: {path_key}")
    return path_key

def get_column_types_df(df):
    lst_column_types = []
    for col in df.dtypes:
        lst_column_types.append({'col': col[0], 'type': col[1]})
    return lst_column_types

def cast_dataframe_schema(args, client, dfin):
    bucket = args["land_bucket"]
    file = f'{get_file_key(args, "schema")}.json'

    result = client.get_object(Bucket=bucket, Key=file)
    text = result["Body"].read().decode()
    schema = json.loads(text)

    df = dfin.select([when(col(c )=="" ,None).otherwise(col(c)).alias(c) for c in dfin.columns])

    for column in schema:
        fieldname = column['column_name']
        fieldtype = column['data_type']

        # df.printSchema()

        # print(f"fieldname: {fieldname}, fieldtype: {fieldtype}")


        if 'int' in fieldtype:
            df = df.withColumn(fieldname, col(fieldname).cast(IntegerType()))

        elif 'serial' in fieldtype or 'bigint' in fieldtype or 'long' in fieldtype:
            df = df.withColumn(fieldname, col(fieldname).cast(LongType()))

        elif 'numeric' in fieldtype or 'decimal' in fieldtype or \
                'double' in fieldtype or 'float' in fieldtype or \
                'real' in fieldtype or 'money' in fieldtype or \
                'currency' in fieldtype:
            df = df.withColumn(fieldname, col(fieldname).cast(DoubleType()))
        elif 'sys_commit_time' in fieldname:
            df = df.withColumn(fieldname, col(fieldname).cast(TimestampType()))
        elif 'text' in fieldtype:
            # df = df.withColumn(fieldname, udf_text_value(col(fieldname)))
            df = df.withColumn(fieldname, col(fieldname).cast(StringType())) \
                .withColumn(fieldname, regexp_replace(col(fieldname) ,"[\\r\\n]", '')) \
                .withColumn(fieldname, regexp_replace(col(fieldname) ,"[|]", '/')) \
                .withColumn(fieldname, trim(col(fieldname))) \
                .withColumn(fieldname, substring(col(fieldname) ,1 ,15000))
        else:
            df = df.withColumn(fieldname, col(fieldname).cast(StringType())) \
                .withColumn(fieldname, f.regexp_replace(col(fieldname) ,"[\\r\\n]", '')) \
                .withColumn(fieldname, f.regexp_replace(col(fieldname) ,"[|]", '/')) \
                .withColumn(fieldname, f.trim(col(fieldname)))

    return df

def get_result_query(connection_name, str_qry):
    conn_param = get_connection(connection_name)
    conn = pg.connect(
        database=conn_param['database'],
        host=conn_param['hostname'],
        port=conn_param['port'],
        user=conn_param['user'],
        password=conn_param['password']
    )
    conn.autocommit = False
    cur = conn.cursor()

    cur.execute(str_qry)
    rows = cur.fetchall()

    cur.close()
    cur.close()

    return rows

def exec_query_with_commit(connection_name, str_qry):
    conn_param = get_connection(connection_name)
    conn = pg.connect(
        database=conn_param['database'],
        host=conn_param['hostname'],
        port=conn_param['port'],
        user=conn_param['user'],
        password=conn_param['password']
    )
    cur = conn.cursor()

    cur.execute(str_qry)
    conn.commit()
    cur.close()

def delete_files(bucket_name, prefix):
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket_name)
    for obj in bucket.objects.filter(Prefix=prefix):
        s3.Object(bucket.name ,obj.key).delete()

def add_sys_file_date(value):
    return str(value)


@udf(returnType=StringType())
def get_spark_column_str(str):
    # return str.upper()
    return str

@udf(returnType=StringType())
def udf_text_value(value):
    value_without_newline = re.sub(r"\r\n\t", " ", value)
    value_without_quote = value_without_newline.replace('\"', '')
    return value_without_quote[0:16380]

# args = getResolvedOptions(sys.argv, ['technology', 'source_bucket', 'domain', 'destination_bucket', 'date', 'file'])

if __name__ == '__main__':
    client = boto3.client('s3')

    print('Iniciado Job')
    args = getResolvedOptions(sys.argv, [
        'date',
        'source_bucket',
        'land_bucket',
        'domain',
        'technology',
        'file'
    ])

    if args['domain'] == 'singulare' and args['technology'] == 'sftp':
        bucket_temporary = 'captalys-analytics-temporary'
        table = f"singulare_{args['file']}_diario"
        prefix = f"{args['domain']}/data/{args['technology']}/v1/parquet/lake/{table}/sys_file_date={args['date']}/"

        s3_path = f"s3://{args['source_bucket']}/{prefix}"
        s3_path_temp = f"s3://{bucket_temporary}/{prefix}"

    else:
        table = f'{args["database"]}_{args["schema"]}_{args["file"]}'
        table = table.replace('-', '_')

    print(f'Processing table {table}')
    print(f'Reading path {s3_path}')

    table_filter = table.lower()
    str_cmd_schema = f"select column_name, data_type from svv_columns c where table_schema = 'captalys_trusted' and table_name = '{table_filter}' order by ordinal_position "
    rows_schema = get_result_query('asgard-redshift-production-informationSchema' ,str_cmd_schema)

    # INFORMATIO SCHEMA --------------------
    cols_list_information_schema = ''
    list_information_schema = []
    if rows_schema:
        for row in rows_schema:
            if cols_list_information_schema:
                cols_list_information_schema += ',' + row[0]
            else:
                cols_list_information_schema = row[0]
            list_information_schema.append(row[0])

    sparkContext = SparkContext()
    glueContext = GlueContext(sparkContext)
    spark = glueContext.spark_session
    # spark.conf.set("spark.sql.legacy.timeParserPolicy","LEGACY")

    print("Leitura dos parquets raw")
    dynamic_frame_from_s3 = glueContext.create_dynamic_frame_from_options(
        connection_type="s3",
        connection_options = {
            "paths": [s3_path]
        },
        format="parquet",
        transformation_ctx="dynamic_frame_from_s3")

    if dynamic_frame_from_s3 and dynamic_frame_from_s3.count() > 0:
        print('Existe dataframe e linhas no df raw')
        table_schema = get_table_schema(args, client)

        data_frame_raw_s3 = dynamic_frame_from_s3.toDF()
        str_sys_file_date = args['date']
        data_frame_raw_fd = data_frame_raw_s3.withColumn('sys_file_date', get_spark_column_str(lit(str_sys_file_date)))

        data_frame_raw_fd.printSchema()

        print('Conversao da raw')
        data_frame_raw = cast_dataframe_schema(args, client, data_frame_raw_fd)
        print('Df convertido')

        dataframe_merged = None

        data_frame_raw.printSchema()
        data_frame_raw.show(3)

        # trusted
        conn_param_trusted = get_connection('asgard-redshift-production-trusted-owner')
        conn_param_trusted['query'] = f"SELECT * FROM captalys_trusted.{table}"
        conn_param_trusted
            ['redshiftTmpDir'] = 's3://captalys-analytics-temporary/aws/tempdir/v1/all/lake/redshift/analytics/datalake_dw/schema'
        print('Buscando dataframe trusted dynamic')
        dynamicFrame_trusted = glueContext.create_dynamic_frame.from_options('redshift', conn_param_trusted)
        print('Dataframe trusted buscado')

        data_frame_raw.printSchema()
        data_frame_raw.show()

        print('Contem dados na trusted - Iniciando if merge')
        # dynamicFrame_trusted.printSchema()

        dataFrame_trusted = dynamicFrame_trusted.toDF()
        print('Conversao do df trusted')
        dataFrame_trusted = cast_dataframe_schema(args, client, dataFrame_trusted)
        print('Df trusted convertido')

        print('Iniciando fors trusted - columns')
        # adiciona colunas na raw caso não exista
        for column in [column for column in dataFrame_trusted.columns if column not in data_frame_raw.columns]:
            data_frame_raw = data_frame_raw.withColumn(column, lit(None))

        # remove others columns
        for column in [column for column in data_frame_raw.columns if column not in dataFrame_trusted.columns]:
            print('As colunas abaixo não existem, favor validar a necessidade da inclusão!')
            print(column)
            data_frame_raw = data_frame_raw.drop(col(column))

        print(str_sys_file_date)

        data_frame_raw.createOrReplaceTempView("df_merged_latest")
        data_frame_raw_filter_val  = spark.sql \
            (f"""select sys_file_date,count(*) as qtd from  df_merged_latest group by sys_file_date""" )
        data_frame_raw_filter_val.show()

        df_merged_latest = data_frame_raw

        print('Ordenando colunas')
        df_ordened = df_merged_latest.select(*list_information_schema)
        df_merged_latest.printSchema()
        df_merged_latest.show()

        print('Deletando arquivos no bucket')
        delete_files(bucket_temporary, prefix)

        df_ordened.printSchema()
        dyf = DynamicFrame.fromDF(df_ordened, glueContext, 'dyf')

        print('Escrevendo os csvs')
        glueContext.write_dynamic_frame.from_options(
            frame = dyf,
            connection_type='s3',
            connection_options = {"path": f'{s3_path_temp}'},
            format="csv",
            format_options={
                "separator": "|"
            }
        )

        print('CSVs escritos no S3')

        str_cmd = f"""   
        begin;
            delete from captalys_trusted.{table} where sys_file_date = '{str_sys_file_date}';
        commit;"""

        print(f"Excluindo reginstros com sys_file_date = '{str_sys_file_date}'")
        exec_query_with_commit('asgard-redshift-production-trusted-owner', str_cmd)

        print('Iniciando o copy para trusted')
        str_cmd_copy = f"""
        COPY captalys_trusted.{table} (
          {cols_list_information_schema}
        )
        FROM
          '{s3_path_temp}' 
        IAM_ROLE 'arn:aws:iam::197342528744:role/CaptalysRedshiftCustomizable' 
        DELIMITER as '|' FORMAT AS csv IGNOREHEADER 1;
        """
        exec_query_with_commit('asgard-redshift-production-trusted-owner', str_cmd_copy)
        print('Finalizado o copy')
        print('Finalizado Job')


    else:
        print('Não existe dataframe')
