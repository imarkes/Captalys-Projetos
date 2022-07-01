# -*- coding: utf-8
import boto3
import zipfile
from datetime import *
from io import BytesIO
import sys
import tarfile
import os

# from awsglue.utils import getResolvedOptions


aws_access_key_id="ASIAS34UIATUPL3SG5QI"
aws_secret_access_key="CCfvFDZURGVe6R5hzpFuATEvN6/3O4rnglFkivVl"
aws_session_token="IQoJb3JpZ2luX2VjENv//////////wEaCXNhLWVhc3QtMSJIMEYCIQC1ZWFr2w2wK6YG+O9dh+oie/Es8P0RqvW2DJgS4xB5ZgIhAMSg59ambn6NYV57WubQzjQySA1s4wPFrQYmhCIfZIxtKqsDCPT//////////wEQARoMMTk3MzQyNTI4NzQ0Igz25sdmARoTtQ3+6zsq/wKSnObu6r6cz+SnsSJLbSTrFnmo6ssidJxygqubCXE0ZlRfT4eHoh0VL/jLaPDIN/rlAretwg9xkxp6KXqzbcxpHQWn7OrrxqKITQyD0VAEZY9IuOBX8vAEF8tNZ8TPqpdqiMhlnk3L2q/C5lFWGr5geCr/XdY4yhMMKib2DqVnm1bH7OqR88Av5gIVemeoLbZda4vB+WDWLiEIWesLpheIpwlT1zKr7FRBOMliLTekLdcC8FbvjkUXozkCBJuNuLHVWvFnrVPCUaq4hGj9QqzB7nV17nDsi72qYOtL9cNj6xg7vake/u4/zW1RxVE78uD38RBZHN3VgyDyf5sz6lkU+QVorc7PogFd3ljuOPwBBaYCf5rZGmtIn42+tkRzzLi7NTvSEbDqU4J3m2dW65i9vLxsjwycYKmciKUOuY1VroyzxQ6JyxKP4C2WFoZtSjkiHwL1uA9VPGzKafiK9UGpjd6xUHwof1kVE6J8shNfIETgVtDDNMejSjg7utfY1zCmnO2VBjqlAetJgFxlcOLXdXj1CVCVZn9sTNN4H2s1GLgnOS+m6BzUKgph1qeP9UBwYLt4Qmjrj30zecbDL638+yppMU5cURQBI51+xQZ7GMDnkA/faMnCDxrIFzgCPE/suyg5qlGOsyzzyx8GmBdmOVyh+e93RJS3zllmjGvmnw5GenjnlIFUda3nJ57c1DALV7Ve4eH76m/LSdYwI7B/vTwyBV8FDY6ZeyUJpg=="
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
    bucket_name='captalys-analytics-land-production',
    data="2022/06/27",
    docs="aquisicao",
    adm="brl"
)


def get_bytes_by_key(bucket_name, key):
    result = s3_client.get_object(Bucket=bucket_name, Key=key)
    byt = result["Body"].read()
    return byt


def send_data_to_s3(body_data_bytes, key_file_name) -> None:
    s3_client.put_object(
        Body=body_data_bytes,
        Bucket=glue_args["bucket_name"],
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


def unzip_files(adm=None, data=None, src=None, dest=None):
    """Extrai arquivos unzip e salva no buket destino."""
    data = data.replace("-", "/")

    prefix_path_zip = f"{adm}/data/sftp/v1/zip/generic/{src}/{data}/"

    response = get_contents_by_prefix(glue_args['bucket_name'], prefix_search=prefix_path_zip)
    for objs in response:

        if objs['Size'] > 0:
            try:
                # Extrai arquivos *.zip
                byt = get_bytes_by_key(glue_args['bucket_name'], objs['Key'])
                buffer = BytesIO(byt)
                unziped = zipfile.ZipFile(buffer, 'r')

                for filename in unziped.namelist():
                    key_str_filename = f"{adm}/data/sftp/v1/csv/lake/{dest}/{data}/{filename}"
                    print(f"Reading file {key_str_filename} in zip")
                    body_data_bytes = unziped.open(filename).read()

                    # Envia arquivo para S3
                    send_data_to_s3(body_data_bytes, key_str_filename)
                    print(f"File {key_str_filename} sended to S3")

            except TypeError as e:
                raise 'File no type: *.zip'

    print(f"Done unzip")


if __name__ == "__main__":
    # dest = liquidacao, aquisicao, estoque
    unzip_files(glue_args['adm'], glue_args['data'], glue_args['src'], glue_args['dest'])

    # elif adm == 'vortx':
    #     prefix_path_zip = f"{adm}/data/sftp/v1/zip/generic/{fidc}/{docs}/{data}/"
    #     print('Unziping file:-> ', prefix_path_zip)
    #
    #     response = get_contents_by_prefix(glue_args['bucket_name'], prefix_search=prefix_path_zip)
    #     for objs in response:
    #
    #         if objs['Size'] > 0:
    #             byt = get_bytes_by_key(glue_args['bucket_name'], objs['Key'])
    #             buffer = BytesIO(byt)
    #             unziped = zipfile.ZipFile(buffer, 'r')
    #
    #             for filename in unziped.namelist():
    #                 key_str_filename = f"{adm}/data/sftp/v1/csv/lake/{fidc}/{docs}/{data}/{filename}"
    #                 print(f"Reading file {key_str_filename} in zip")
    #                 body_data_bytes = unziped.open(filename).read()
    #                 print(f"Sending file {key_str_filename} to S3")
    #                 # send_data_to_s3(body_data_bytes, key_str_filename)
    #                 print(f"File {key_str_filename} sended to S3")
    #     print(f"Done unzip VORTX")
    # else:
    #     return []





