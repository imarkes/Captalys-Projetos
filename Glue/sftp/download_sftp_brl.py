# -*- coding: utf-8
from io import BytesIO, StringIO
import boto3
import os
import paramiko as paramiko
from connection_sftp import SftpConnector

aws_access_key_id = "ASIAS34UIATUHJOEIRB6"
aws_secret_access_key = "eGHy+WkhHqKa0mVNNkTULyIVm+erw1vEVHoBZACF"
aws_session_token = "IQoJb3JpZ2luX2VjEMr//////////wEaCXNhLWVhc3QtMSJHMEUCIQC10mRiaFujYf3XatCZg8/0D3fm4ko4fbnvTyPizQQR8wIgAN6HsblicGpyDu0iJ5+jHQ36Zh8pjFlSGF7k6O4eLXsqqwMI8///////////ARABGgwxOTczNDI1Mjg3NDQiDPsmjJh9mvGu3NkYoSr/Asoo4lZQ70psxKxOYG5sM3aK9BgU0OF3H7dIxgrX7hjprQu0lyiwgq0Nypl/ShAcEcJPIiMKPkpJZHpqOo7CKfjWzePDyTL2bXkE5aQ52e0SscS+D+IvSuY9sjqtaSGsj8dpiAu5quO7SKBJED60hXmfgv14enwM1kZXPwcsO7b8kM+3qyk0CpY462FU+JzBEukrA8U0fCWbXszKHHw3VFUi8SvOoIzE350jrLaW2jLeEyywyKcLr/2Z2fLsuAapLNWhtJYzUiuSCdwNJnE4JLyJ6GRQID0qLHNMLf/cL7hZ6MnabuJmtS7eZftmhdNrh+dypIdNF1xF1/V7kzhVKKC8GQ46S/kbKheDNd1/nCSn3fy4bcjwsVS+T8mTLl10AJ66BAXvE5ZZtrbSJSaRiPulQhL5RAjJnkYhPbBPyFeZ1E2xgZRIvaWFUnzVf1vPEHygZXH5iMOd9oI7U2YgVC6oGDgrcq81Y23icA6ebgt3n5RPOkizg4SSLABZIEQ2MIPVoZYGOqYBcALTW77IWkwd62cAmZvlARh4PrgNRxIsoi7vHhsQlTUORmxwCdSTEBkBsAqy10eJewpDDDgKYCyP+kCVJz7/qcigIWIu/jS6T+hps10SglgePywBJ+K8gfsGrFEhOGfqy1yAGK6JPM2MOHYB9zF8d6X9V6y0+iqiiksD39yNrpv7Gia1UlCPMEcZxZ5l9gv4/ANeAezQl99SBbcKC0O7RWmzIM4btA=="
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
    private_key='/home/ivan/Captalys/Projetos/Docs/acessoBRL.pem',
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


def delete_files(bucket_name, prefix):
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket_name)
    for obj in bucket.objects.filter(Prefix=prefix):
        s3.Object(bucket.name, obj.key).delete()


class ExtractSftpBrl:
    """Baixa arquivos do sftp e envia para o S3"""

    def __init__(self, hostname=None, port=None, username=None, private_key=None, password=None):
        self.__conn = SftpConnector(hostname=hostname, port=port, username=username, private_key=private_key)
        self.__password = password
        self.brl = self.__conn.password_connection(self.__password)
        self.s3_files_data = {}

    def brl_list_path_files(self, domain: str, path: str, date: str, prefix: str):
        """Lista os arquivos do diretorio sftp e envia para S3"""
        period = date.split("-")
        str_date = date.replace('-', '')

        with self.brl.cd(path):
            try:
                for filename in self.brl.listdir():
                    if (str_date in filename) and (prefix.upper() in filename):
                        key_str_filename = f'{domain}/data/sftp/v1/zip/generic/{prefix}/{period[0]}/{period[1]}/{period[2]}'

                        flo = BytesIO()
                        buffer = self.brl.getfo(f'/{path}/{filename}', flo)
                        print(filename)
                        flo.seek(0)
                        print('Enviando: ', f'{key_str_filename}/{filename}')
                        if key_str_filename:
                           delete_files(glue_args["bucket_name"], f'{key_str_filename}')
                        send_data_to_s3(flo, f'{key_str_filename}/{filename}')

            except FileNotFoundError as e:
                print('[]', e)


if __name__ == "__main__":
    # str_key = get_text_by_key('captalys-analytics-land-production', 'brl/Key/sftp/v1/key-pem/acessoBRL.pem')
    # private_key_file = StringIO()
    # private_key_file.write(str_key)
    # private_key_file.seek(0)
    # private_key = paramiko.RSAKey.from_private_key(private_key_file, password=glue_args['password'])
    brl = ExtractSftpBrl(hostname=glue_args['host'], port=22, username=glue_args['username'],
                         private_key=glue_args['private_key'],
                         password=glue_args['password'])

    # Parametros arquivos,
    # liqbaix, aquisicoes, estoquediario
    brl.brl_list_path_files('brl', 'estoque_diario', '2022-07-07', 'aquisicoes')

# brl_host = 'sftran.brltrust.com.br'
# brl_username = 'captalys'
# brl_private_key = "/home/ivan/Dev/glue_jobs/sftp/acessoBRL.pem"
# blr_password = "w@ferreira5"
