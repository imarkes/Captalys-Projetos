# -*- coding: utf-8
from io import BytesIO, StringIO
import boto3
import os
import paramiko as paramiko
from connection_sftp import SftpConnector

aws_access_key_id = "ASIAS34UIATUE2CWSRVV"
aws_secret_access_key = "p4d+DZE+qMpicp4v3KOHP5PQtWRhZiISrJhv3Pia"
aws_session_token = "IQoJb3JpZ2luX2VjEFQaCXNhLWVhc3QtMSJGMEQCIGnu2UhteGNkbqD7gCLswTLntrQ/0OOCGhLF+n5bmha4AiAKDxUnWOuyIXsfh2g6sjkiYTyMm8ZREOaJmtkTiZUD1yqrAwiN//////////8BEAEaDDE5NzM0MjUyODc0NCIMAQ3nSW6A+MXOsNRqKv8CMT3/FpXhIDCUt5POpqF2+5ss9niCYpywl05Kel9gETi2AxRDG0wsI8dBLqfT9UklWGFEo944tMc1xUciF3OlDuYnH+PBz9lRrNpfEpsJSnb1LImGo6WCicFRVW5ok1F/Ttg3Z7iL/tMlvbOChHi0wYWsnLnLk7+VcMNtF1ofcj07kbCN22E0DrVWdPegyyX68RXPTRUO3dZ/Bk5ZhbYeB11+41y/GTtRUmi96DmiKPaPxykPAKCVrzw0vgDcNUhpKDsaNRXMa1zY4etyQUP/hfyg4lECOaww2q3ZjcEQyyPr04w/TnA46SJxzjXtZeBVBMlQfEHP41Smmhl5Xp/AllXCTjSz90D+QmhJPocH3CzT0yEll7MX1A6TmX/clpWI33T8LSub5Z0bRx0ubDu/J+2izd/QIughetwZLWciT0y/x8/r4hZv6a44/IXnc6uKsu7gvaXbciLOoZKs4P52xspIhIKBfb5c3ytscEdC+TxUOpA2n1IEyyTJiH7fhvAwm4vAlgY6pwFBcOeQDolRrffJT0nEsVscy4k1V/jW7bm42y0skoxlk5cIy4EXCqBy/UlCFx7TFS/+9kYBHtFp3yz/o1FC09cRFy4Uta92MuuZhLlsk6HIA5ibBnTqmZlydWxbc3LEEcNFlJim55TXv4favejzxA29brS2tOYe/9jZKhuiNDgx6TcfTACwuFjQzsORCMujSM6/Fpgn39i8ZedNPvdgKx1VSr5nrZATdQ=="
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

glue_args = dict(
    dest_bucket="captalys-analytics-land-production",
    source_buket='captalys-analytics-adms-production',
    date='2022-07-11',
    file='aquisicao',
    domain='brl',
    tecnology='sftp'

)


def send_data_to_s3(file_obj, key_file_name) -> None:
    """" Envia arquivos binarios para o buket"""
    s3_client.put_object(
        Body=file_obj,
        Bucket=glue_args['dest_bucket'],
        Key=key_file_name
    )


def get_contents_by_prefix(bucket_name, prefix_search):
    response = s3_client.list_objects_v2(
        Bucket=bucket_name,
        Prefix=prefix_search
    )
    if 'Contents' in response and isinstance(response['Contents'], list) and len(response['Contents']) > 0:
        return response['Contents']
    else:
        return []


def get_bytes_by_key(bucket_name, key):
    result = s3_client.get_object(Bucket=bucket_name, Key=key)
    byt_obj = result["Body"].read()
    return byt_obj


def download_zip_brl_sftp_local(domain=None, file=None, date=None):
    prefix_source_files = f"{domain}/sftp/"
    str_date = date.replace('-', '')
    year = date.split('-')[0]
    month = date.split('-')[1]
    day = date.split('-')[2]
    name_file = ''

    # lista os arquivos do diretorio
    response = get_contents_by_prefix(glue_args['source_buket'], prefix_search=prefix_source_files)

    if 'liq' in file:
        name_file = 'liqbaix'.upper()
    elif 'aquisic' in file:
        name_file = 'aquisicoes'.upper()
    elif 'estoque' in file:
        name_file = 'estoquediario'.upper()

    prefix_dest_files = f'{domain}/data/sftp/v1/zip/generic/{file}/{year}/{month}/{day}'
    for objs in response:

        if objs['Size'] > 0:
            if str_date in objs['Key'] and name_file in objs['Key']:
                filename = str(objs['Key']).split('/')[2]
                # Converte em bytes
                files_byt = get_bytes_by_key(glue_args['source_buket'], objs['Key'])
                # Envia arquivos.
                print('Enviando para ->:', f'{prefix_dest_files}/{filename}')
                send_data_to_s3(files_byt, f'{prefix_dest_files}/{filename}')


download_zip_brl_sftp_local(glue_args['domain'], glue_args['file'], glue_args['date'])
