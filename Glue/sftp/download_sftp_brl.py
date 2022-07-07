# -*- coding: utf-8
from io import BytesIO, StringIO
import boto3
import os
import paramiko as paramiko
from connection_sftp import SftpConnector
from pathlib import Path

aws_access_key_id = "ASIAS34UIATUK43HFZQO"
aws_secret_access_key = "a998Lyg57kn8f/dVjVz2wxC2MsQ8YteXxvEptl/S"
aws_session_token = "IQoJb3JpZ2luX2VjEML//////////wEaCXNhLWVhc3QtMSJGMEQCIB3MLqqfNhhXAinWqLKc1xktg1QFVQZxy7nR+arp7qssAiAS4uQAqZ44+Vaf0krRhUffOV64rcFSMwL5Ha+jnvf6YiqrAwjb//////////8BEAEaDDE5NzM0MjUyODc0NCIMDb0KbKQJBFEINNVDKv8Ckh0J1EilCjREPAiFT8SFSlILnOgbO/06A4WsjKopWB5hSXsHbuTwnEFLH/ZHWVynaDpYWojdDGlaWwxVERLIg6Yexnnd+CSzGku/cLpnx3Ptld8x4g4i7mS3LMpsIwBPxDSqgwapV2b7a0X5nPTkUSAmTHy/OECl/PKWkzBhfP8+4Ws1W7B6X/n/OkuY5WeQGE0uUNbmqIRIRaNtrilFT3xekJtPOF88hu9WHMJIXhbHYdD0rDCk+D/NJSeDObgrGDStw7o+tBonZCUXRGXNhehTpBDn/mxGSlCvFl5qpOw9SQP5lJPl6Xtg1SfvBkmPtgAn0QMEGlRgIODeSi4NidLXVG+1ITVB2yWQMbZgxxpUYVhO2mxPg/s6Bv5NcV7nXX7Or2/D8Pt28qc/NnMIEvMNGCuVdzsNI2dvDydsPDEw1YBqrpwgPubvb9urp22liJ6+kzJ1T5adRKI+feJ7QYLlhPrerAxgM0Pv4JzwYSbjm233L7p51beG+V2Krwww3tvnlQY6pwHe9aTAl8g+jmynlfUS8hWIL8UWHLhfKJ4+7HKiY2hO5cph+AHtkQVrj/ytCIWLRJfwl3zRM5OfXBOVj/FfQvyZo+CIUoDGJ81f1Y8bzkv1xr1Nc7YEUSzVR8Im0YXwVbqltJjr/jvY0bFcjT4d5CRXua/6M/BXZ3vGhcOMA+CCmyvmapkxr1jMvFIpiDZEx+WAMbnMWAbdrPk6fE5X2NCTJkjiBqyexQ=="

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
    bucket_name="bucket_name",
    private_key='/home/ivan/Dev/glue_jobs/sftp/acessoBRL.pem',
    port=22,
    host="sftran.brltrust.com.br",
    password="w@ferreira5",
    username="captalys"
)


def up_files_to_s3(filename, key_destination):
    """Envia arquivos locais para o bucket"""
    s3_client.upload_file(Filename=filename,
                          Bucket='captalys-analytics-land-production',
                          Key=key_destination)


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


class ExtractSftpBrl:
    """Baixa arquivos do sftp e envia para o S3"""

    def __init__(self, hostname=None, port=None, username=None, private_key=None, password=None):
        self.__conn = SftpConnector(hostname=hostname, port=port, username=username, private_key=private_key)
        self.__password = password
        self.brl = self.__conn.password_connection(self.__password)
        self.s3_files_data = {}

    def brl_list_path_files(self, adm: str, path: str, day: str, prefix: str):
        """Lista os arquivos do diretorio sftp e envia para S3"""
        path_s3_brl_files_prefix = f'{adm}/data/sftp/v1/zip/generic/'
        period = day.split("-")
        s3_uri = []
        name_files = []

        with self.brl.cd(path):
            directory_structure = self.brl.listdir_attr()
            print(f'Arquivos no path: {adm.lower()}{self.brl.pwd}')

            # Lista os arquivos no diretorio
            for attr in directory_structure:
                try:
                    if (day in attr.filename) and (prefix.upper() in attr.filename):
                        name_files.append(attr.filename)
                        s3_uri.append(
                            f'{path_s3_brl_files_prefix}{prefix}/'
                            f'{period[0]}/{period[1]}/{period[2]}'
                            f'/{attr.filename}')

                        self.s3_files_data['key_name'] = name_files
                        self.s3_files_data['s3_uri'] = s3_uri

                except FileNotFoundError as e:
                    raise ('Arquivos não encontrados com os parametros informados ', e)

        if len(self.s3_files_data) >= 1:
            for file in self.s3_files_data['key_name']:
                flo = BytesIO()
                buffer = self.brl.getfo(f'/{path}/{file}', flo)
                flo.seek(0)
                send_data_to_s3(flo,
                                f'{path_s3_brl_files_prefix}{prefix}/{period[0]}/{period[1]}/{period[2]}/{file}')

        else:
            print(f'Arquivos do dia: {day}, vazios')


if __name__ == "__main__":
    str_key = get_text_by_key('captalys-analytics-land-production', 'brl/Key/sftp/v1/key-pem/acessoBRL.pem')
    private_key_file = StringIO()
    private_key_file.write(str_key)
    private_key_file.seek(0)
    private_key = paramiko.RSAKey.from_private_key(private_key_file, password=glue_args['password'])
    brl = ExtractSftpBrl(hostname=glue_args['host'], port=22, username=glue_args['username'], private_key=private_key,
                         password=glue_args['password'])

    # Parametros arquivos,
    # liqbaix, aquisicoes, estoquediario
    brl.brl_list_path_files('brl', 'estoque_diario', '2022-06-28', 'aquisicoes')


# brl_host = 'sftran.brltrust.com.br'
# brl_username = 'captalys'
# brl_private_key = "/home/ivan/Dev/glue_jobs/sftp/acessoBRL.pem"
# blr_password = "w@ferreira5"
