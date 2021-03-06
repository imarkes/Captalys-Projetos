# -*- coding: utf-8
from io import BytesIO, StringIO
import boto3
from connection_sftp import SftpConnector

from pathlib import Path

aws_access_key_id = "ASIAS34UIATUAZRP4JJP"
aws_secret_access_key = "yr0zJXgu4zx9yZrb/6cX/J0GWFyc6gDebHqwY0Aa"
aws_session_token = "IQoJb3JpZ2luX2VjEGYaCXNhLWVhc3QtMSJGMEQCICASryxV81eMQoQ8uGCt6Zi/oC78zHgO+dHasH5uap7MAiA8aF3VZfnfCI5pIMGZtdHsnLRfISRCUf6cCQIzFyc6LCqrAwiP//////////8BEAEaDDE5NzM0MjUyODc0NCIMQAuraDLUGdfjU8PYKv8CKOQekyHrhkkr09Wg9kcR6Hf15Mc1Vv8Vmm0ga4KxTuGi/F2IwB3+OPm6jJv3Z294MeTJWEhrrFsdKdvwDkuRvNk5lzkrPu71ZT/LT4WmWEWKlomtXvKkkeoibmpmvPo9NalY8DyORwXP2+0zQVj6z+xhq8oZRGY2mb+qMxZuGNwLbj3mM902V2BCTx84M7VB2ALSmXv+zdTSbgTwxGr9xjIOmekRfQAiw+EXH7KFeufLHGzJkAaTSHSDhenBuC6XLH9eeoKm/9zlwwmIHfXvzsaTtN6IPwC1LKeJ0PZ5Psrjy9ymM4lVO2sQ38jGA0InC1kD5eNrkTUhaDjAwELEXegHZOlO5B9iH0ByzKrSjQcwK9RkiZJOU5YbtFUT1fofN7+Ys6JqNo4xDt4t5nYWlXExfS8DbiXFn6As34DIR2JpWGqfFscUf/ywkQ+EYDsPKiOLDk640qQonwyq/V6ZwuUFQfCphEkdiyt+97jaRklmm2eGON7vcZqjG4vCxhYw6NiLlgY6pwFgIq5oDF5CgKrx9k8VNHGwZn5h4OVrcpgPv+OghmnQ13YnvVpbRvNBt+l9uZVzA+CwPpVqF0MZK5voD+yYjLJ4ZZV+C6Ixdd2DXnSAjjKZLhIjBn7vjEavjBEYB6FryEH12vRtxoOhHH/36FBy4kjOCkF4Q/BJ+DLitX/ht26tAqw+anCHU7D7oD1BqBDY+zn5Ivja36NnicJNTP9uSMcVt0n0VpeR1A=="

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


def send_data_to_s3(file_obj, key_file_name) -> None:
    """" Envia arquivos binarios para o buket"""
    s3_client.put_object(
        Body=file_obj,
        Bucket='captalys-analytics-land-production',
        Key=key_file_name
    )


def get_text_by_key(bucket_name, key):
    """Decripta  o objeto"""
    result = s3_client.get_object(Bucket=bucket_name, Key=key)
    text = result["Body"].read().decode()
    return text


class ExtractSftpVortx:
    def __init__(self, hostname=None, port=None, username=None, private_key=None, password=None):
        self.__conn = SftpConnector(hostname=hostname, port=port, username=username, private_key=private_key)
        self.vortex = self.__conn.default_connection()
        self.__password = password
        self.s3_files_data = {}

    def vortx_list_path_files(self, domain: str, fidc: str, folder: str, data: str, docs=None):
        days = data.split('-')[0]
        month = data.split('-')[1]
        year = data.split('-')[2]
        new = data.replace('-', '.')

        with self.vortex.cd(f'/ftp-vortx/{fidc}/{folder.capitalize()}/processados/'):

            for filename in self.vortex.listdir():
                if (new in filename) and (docs.capitalize() in filename):
                    key_str_filename = f"{domain}/data/sftp/v1/xlsx/lake/{folder}/20{year}/{month}/{days}/{filename}"

                    flo = BytesIO()
                    buffer = self.vortex.getfo(f'/ftp-vortx/{fidc}/{folder.capitalize()}/processados/{filename}', flo)
                    flo.seek(0)
                    print('Enviando: ', key_str_filename)
                    send_data_to_s3(flo, key_str_filename)


vortex_host = 'sftp.captalys.io'
vortex_username = 'vortx'
vortex_private_key = "/home/ivan/Captalys/Projetos/Docs/vortx_rsa.dat"
port = 22

vortx = ExtractSftpVortx(vortex_host, port, vortex_username, vortex_private_key)

M = '07'
Y = '22'

datas = [
    f'01-{M}-{Y}',
    f'02-{M}-{Y}',
    f'03-{M}-{Y}',
    # f'04-{M}-{Y}',
    # f'05-{M}-{Y}',
    # f'06-{M}-{Y}',
    # f'07-{M}-{Y}',
    # f'08-{M}-{Y}',
    # f'09-{M}-{Y}',
    # f'10-{M}-{Y}',
    # f'11-{M}-{Y}',
    # f'12-{M}-{Y}',
    # f'13-{M}-{Y}',
    # f'14-{M}-{Y}',
    # f'15-{M}-{Y}',
    # f'16-{M}-{Y}',
    # f'17-{M}-{Y}',
    # f'18-{M}-{Y}',
    # f'19-{M}-{Y}',
    # f'20-{M}-{Y}',
    # f'22-{M}-{Y}',
    # f'22-{M}-{Y}',
    # f'23-{M}-{Y}',
    # f'24-{M}-{Y}',
    # f'25-{M}-{Y}',
    # f'26-{M}-{Y}',
    # f'27-{M}-{Y}',
    # f'28-{M}-{Y}',
    # f'29-{M}-{Y}',
    # f'30-{M}-{Y}',
    # f'31-{M}-{Y}',
]


#'FIDC Merchant'
# FIDC Onix
# FIDC Citrino


for day in datas:
    vortx.vortx_list_path_files('vortx', 'FIDC Onix', 'estoque', day, 'estoque')
