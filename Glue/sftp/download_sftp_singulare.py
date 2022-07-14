# -*- coding: utf-8
from io import BytesIO, StringIO
import boto3
from connection_sftp import SftpConnector

aws_access_key_id="ASIAS34UIATUI7QD2KFZ"
aws_secret_access_key="IdT7hpraHHiTOxTvb+4cdq40LkT16W7m4n+N2tk6"
aws_session_token="IQoJb3JpZ2luX2VjELL//////////wEaCXNhLWVhc3QtMSJIMEYCIQCr5eNbZJaZVyaHtlaaI5usRNtihfKZbF1hFdoVmuMdzAIhAKOuUpkZpPgmNqpJtjQLNaIbS9TU91dFtT7wtWdKYlF2KqsDCNr//////////wEQARoMMTk3MzQyNTI4NzQ0Igy1JuJ6lsmZQr6k06cq/wL5ft2Yesbewqaa0ZmWvIY5eQXPl+dIdPHV8xfUvL2bdIA0xVRSOyVdWAgy+CNI7TCIDAn73TDdS9u039MaJ0rED2NBS79cVUzqQ6gqigjyfFx2hMrri/gCmgGxRjuZkZF+dEAM+kS8ZleK7bVMYHL8sM0k1vDSZElmljBRAKLvjQIO6TCuIYHySOitF2OCnSKWhn99GQBm9C6NJ/DIEOo6WDpahVHu4T5cqBtGKcYdOfREX57WG/aWyFwwLV3D0Kw2uQPBB98CufGtMFEBM6IC+PStwxEkluayyHs1zg1ab1WNu2+g7K83p/riZP6K4vPKu4zN+6CQOyjR6K0z8LIq95en7K6/BiEsGwqTgHySlA9GK3HVQjU5h4j9ojVWoMA5/Z+EwJEUBXpwYVVYhD2ep0mKKSvliT9f0FgodC6MjauFH3vMUqCwOaazwUb/dcpHNc20K35D9OLVvNczqlbdE6Wau9EODQwVqsOVyfV3wE0glCC1jv2GrRVLibWjTzD3q5yWBjqlAalgtlyWHAwojfmmiQ+bqt15Drdj0wD6GUvPspKYn6Vx5R9to1ml3KFGllbPRugPUA+NAkd0spLNpqv0nxfYionak/N5gFsPVwDqr5xJG/155EpZRUsaR5rzEteMyAvYZ6SlSs1zYW6JOPRMFyMI+0C+zSDw1zsNV6hHtUtYqi/lL0akkMBAT889Bo17NDrMFuOGPQeHLKDe4DS6YLvNl51e8vsWtA=="

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

        # Lista os arquivos do diretorio
        self.singulare.cd(f'/sftp-captalys/SIMPLIC/{folder.upper()}')

        for filename in self.singulare.listdir():
            if (str_date in filename) and (docs.upper() in filename):
                if docs == 'liquidados':
                    docs = 'liquidacao'
                    key_str_filename = f"{domain}/data/sftp/v1/csv/lake/{docs}/{year}/{month}/{days}/{filename}"
                else:
                    key_str_filename = f"{domain}/data/sftp/v1/csv/lake/{folder}/{year}/{month}/{days}/{filename}"

                flo = BytesIO()
                buffer = self.singulare.getfo(f'/sftp-captalys/SIMPLIC/{folder.upper()}/{filename}', flo)
                flo.seek(0)
                print('Enviando: ', key_str_filename)
                #send_data_to_s3(flo, key_str_filename)


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

    ]
    #liquidados
    #estoque
    #aquisicao
    for execute_date in datas:
        print('Verificando dia: ', execute_date)
        socopa.singulare_list_path_files('singulare', 'estoque', execute_date, 'liquidados')
