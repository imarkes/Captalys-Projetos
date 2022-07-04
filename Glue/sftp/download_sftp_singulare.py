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


class ExtractSftpSimgulare:
    def __init__(self, hostname=None, port=None, username=None, private_key=None, password=None):
        self.__conn = SftpConnector(hostname=hostname, port=port, username=username, private_key=private_key)
        self.singulare = self.__conn.default_connection()
        self.__password = password

    def singulare_list_path_files(self, domain: str, folder: str, date: str, docs=None):

        days = date.split('-')[2]
        month = date.split('-')[1]
        year = date.split('-')[0]
        str_date = date.replace('-', '')

        # /sftp-captalys/SIMPLIC/ESTOQUE
        self.singulare.cd(f'/sftp-captalys/SIMPLIC/{folder.upper()}')

        for filename in self.singulare.listdir():
            if (str_date in filename) and (docs.upper() in filename):
                key_str_filename = f"{domain}/data/sftp/v1/csv/lake/{folder}/{year}/{month}/{days}/{filename}"

                flo = BytesIO()
                buffer = self.singulare.getfo(f'/sftp-captalys/SIMPLIC/{folder.upper()}/{filename}', flo)
                flo.seek(0)
                print('Enviando: ', key_str_filename)
                send_data_to_s3(flo, key_str_filename)


glue_args = dict(
    host='sftp.captalys.io',
    port=22,
    username='socopa',
    private_key='/home/ivan/Captalys/Projetos/Docs/socopa-singulare.pem'
)

if __name__ == '__main__':

    socopa = ExtractSftpSimgulare(glue_args['host'], glue_args['port'], glue_args['username'], glue_args['private_key'])

    M = '01'
    Y = '2021'

    datas = [
        f'{Y}-{M}-01',
        f'{Y}-{M}-02',
        f'{Y}-{M}-03',
        f'{Y}-{M}-04',
        f'{Y}-{M}-05',
        f'{Y}-{M}-06',
        f'{Y}-{M}-07',
        f'{Y}-{M}-08',
        f'{Y}-{M}-09',
        f'{Y}-{M}-10',
        f'{Y}-{M}-11',
        f'{Y}-{M}-12',
        f'{Y}-{M}-13',
        f'{Y}-{M}-14',
        f'{Y}-{M}-15',
        f'{Y}-{M}-16',
        f'{Y}-{M}-17',
        f'{Y}-{M}-18',
        f'{Y}-{M}-19',
        f'{Y}-{M}-20',
        f'{Y}-{M}-22',
        f'{Y}-{M}-22',
        f'{Y}-{M}-23',
        f'{Y}-{M}-24',
        f'{Y}-{M}-25',
        f'{Y}-{M}-26',
        f'{Y}-{M}-27',
        f'{Y}-{M}-28',
        f'{Y}-{M}-29',
        f'{Y}-{M}-30',
        f'{Y}-{M}-31',
    ]

    for day in datas:
        print('Verificando dia: ', day)
        socopa.singulare_list_path_files('singulare', 'estoque', day, 'estoque')
