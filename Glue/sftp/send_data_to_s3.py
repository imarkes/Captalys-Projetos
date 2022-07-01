# -*- coding: utf-8
import os
from io import BytesIO, StringIO
import boto3


aws_access_key_id="ASIAS34UIATUB7FJR4VI"
aws_secret_access_key="t8Pf4D6Lc7zLBxQWzoihiMGLXeLhMbuDofv5wW0k"
aws_session_token="IQoJb3JpZ2luX2VjEB4aCXNhLWVhc3QtMSJGMEQCID79ovmKLQAAPKbP3Lh4AfpPDn0GtkxmPoPA+QKPyOlkAiAano242aTvnl533w2Gnf4Lvkg0oqk7ZaCpfLL5EvIpHSqiAwhHEAEaDDE5NzM0MjUyODc0NCIMySreVTh6IKAB5Aw2Kv8CBDTkbtDtfaEAiwRdY2+WNSCVhmuRGBd2qUh54lQBA7VJS1CxBQfTwqv7dZhiyi5ejLhUIBgF7kl4j67+wED/cy8KsvDyEehOSz4iOL/ruhrJOXeo51zciUPJt0E6J8WzUQqz/caCl1aYltp8qzq3ABDYNAf2gKanLhVL17rci7ewKPQ3iHqwThtRnxYW3TpJOSBgLL369HT/BW1VBFDXEHpNppZ5XipjeX+NLprRxT9sFr31RfQxcItuoM6dQe9T+KQyDv9g/qPvOtdun9HzOTtVi3SgV7gZ22samlFo8zLXJONuZk8obeect/cN7KSBsFW8dqqTBMfIcff3UY1KQF2ht8binHYJa/EX1vwA7mRWpVbgDahWNI4KrxO+8A3VWKgFoY79lGiCOhVlxdZTNQDGpaUK7bQ9qBaYQ9UxwR/nXa8GdmDbWUo6QdvR73acb/2PaISjsTdp1riiGR9YsOe7jwwv9N3nBE1ADxnuZfauMqsg8uC4DdMAAM2cs/Awj4L8lQY6pwFXsiEINtYID2Vw7DpYNIWOqbfmpMqX49lAUcfHCkSSw7vjh95me5WkDYWKBZRMISAiJ/kBbfmWfBrKJM+ztpeKUZ2LsFD25n6SXIQC6rU0508AAN8AwDNaL0suSHvdK0iub780HCzA2jUImK3EeaNmbuztzipTJsraJhV/nY6FS1qMdnTFBk9Mzwv/rzFbfGdOvFyAJ2jCe9m+hCAHRh6uFl5LbVdzMQ=="
s3_client = boto3.client(
    "s3",
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    aws_session_token=aws_session_token
)


def send_data_to_s3(data_bytes, key_file_name) -> None:
    """
    Read databytes and put in bucket name.
    """
    s3_file = BytesIO(data_bytes)

    s3_client.put_object(
        Body=s3_file.getvalue(),
        Bucket='captalys-analytics-land-production',
        Key=key_file_name
    )


def upload_sftp_files(adm=None, data=None, path=None, docs=None):
    """
    Send local files to Bucket.
    :param adm: brl
    :param data: '1022-01-01'
    :param path: './local/file.txt
    :param docs: estoque
    """

    if adm == 'vortx':
        days = data.split('-')[0]
        month = data.split('-')[1]
        year = data.split('-')[2]
        new = data.replace('-', '.')

        for file in os.listdir(path):
            if new in file:
                key_str_filename = f"{adm}/data/sftp/v1/xlsx/lake/{docs}/20{year}/{month}/{days}/{file}"
                filename = path + file
                f = open(filename, 'rb')
                databytes = f.read()
                print('Enviando: ', key_str_filename)
                send_data_to_s3(databytes, key_str_filename)

    elif adm == 'brl':
        data_path = data.replace("-", "/")
        for file in os.listdir(path):
            if data in file:
                key_str_filename = f"{adm}/data/sftp/v1/csv/lake/{docs}/{data_path}/{file}"
                filename = path + file
                f = open(filename, 'rb')
                databytes = f.read()
                # send_data_to_s3(databytes, key_str_filename)


path = '/home/ivan/Documentos/vortex/'

M = '06'
Y = '22'

datas = [
    f'01-{M}-{Y}',
    f'02-{M}-{Y}',
    f'03-{M}-{Y}',
    f'04-{M}-{Y}',
    f'05-{M}-{Y}',
    f'06-{M}-{Y}',
    f'07-{M}-{Y}',
    f'08-{M}-{Y}',
    f'09-{M}-{Y}',
    f'10-{M}-{Y}',
    f'11-{M}-{Y}',
    f'12-{M}-{Y}',
    f'13-{M}-{Y}',
    f'14-{M}-{Y}',
    f'15-{M}-{Y}',
    f'16-{M}-{Y}',
    f'17-{M}-{Y}',
    f'18-{M}-{Y}',
    f'19-{M}-{Y}',
    f'20-{M}-{Y}',
    f'22-{M}-{Y}',
    f'22-{M}-{Y}',
    f'23-{M}-{Y}',
    f'24-{M}-{Y}',
    f'25-{M}-{Y}',
    f'26-{M}-{Y}',
    f'27-{M}-{Y}',
    f'28-{M}-{Y}',
    f'29-{M}-{Y}',
    f'30-{M}-{Y}',
    f'31-{M}-{Y}',
]

for day in datas:
    upload_sftp_files('vortx', day, path, 'estoque')
