# -*- coding: utf-8
import boto3
import zipfile
from datetime import *
from io import BytesIO
import sys
import tarfile
import os

# from awsglue.utils import getResolvedOptions


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
    bucket_name='captalys-analytics-land-production',
    date="2022-07-14",
    file="aquisicao",
    domain="brl",
    qt_days_reprocessing='5'

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


def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)


def unzip_brl_files(domain=None, date=None, file=None):
    date_path = date.replace("-", "/")

    prefix_path_zip = f"{domain}/data/sftp/v1/zip/generic/{file}/{date_path}"

    response = get_contents_by_prefix(glue_args['bucket_name'], prefix_search=prefix_path_zip)
    for objs in response:

        if objs['Size'] > 0:
            try:
                # Extrai arquivos *.zip
                byt = get_bytes_by_key(glue_args['bucket_name'], objs['Key'])
                buffer = BytesIO(byt)
                unziped = zipfile.ZipFile(buffer, 'r')

                for filename in unziped.namelist():
                    key_str_filename = f"{domain}/data/sftp/v1/csv/lake/{file}/{date_path}/{filename}"
                    body_data_bytes = unziped.open(filename).read()

                    # Envia arquivos
                    print(f"Enviando arquivo ->:  {key_str_filename}")
                    send_data_to_s3(body_data_bytes, key_str_filename)

            except FileNotFoundError as e:
                print('File no type: *.zip', e)

    print(f"Done unzip")


if __name__ == "__main__":
    # Dowload retroativo
    qt_days_reprocessing = int(glue_args["qt_days_reprocessing"])
    date = f'{glue_args["date"]} 00:00:00'
    dateIni = datetime.strptime(date, '%Y-%m-%d %H:%M:%S') - timedelta(days=qt_days_reprocessing)
    dateFim = datetime.strptime(date, '%Y-%m-%d %H:%M:%S')
    for single_date in daterange(dateIni, dateFim):
        print(f'Verificando arquivos do dia: {single_date.strftime("%Y-%m-%d")}')
        date_execute = single_date.strftime("%Y-%m-%d")
        unzip_brl_files(glue_args['domain'], date_execute, glue_args['file'])
