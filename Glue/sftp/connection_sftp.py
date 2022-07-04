import pysftp


class SftpConnector:
    def __init__(self, hostname=None, port=None, username=None, private_key=None) -> None:
        self.hostname = hostname
        self.port = port
        self.__username = username
        self.__private_key = private_key
        self.sftp_conn_password = None
        self.sftp_conn = None

    def default_connection(self):
        cnopts = pysftp.CnOpts()
        cnopts.hostkeys = None

        self.sftp_conn = pysftp.Connection(
            host=self.hostname,
            username=self.__username,
            private_key=self.__private_key,
            port=22,
            cnopts=cnopts
        )
        print("Connection succesfully SFTP... ")
        return self.sftp_conn

    def password_connection(self, password=None):
        cnopts = pysftp.CnOpts()
        cnopts.hostkeys = None

        self.sftp_conn_password = pysftp.Connection(
            host=self.hostname,
            username=self.__username,
            private_key_pass=password,
            private_key=self.__private_key,
            port=22,
            cnopts=cnopts
        )
        print("Connection succesfully SFTP ... ")
        return self.sftp_conn_password


# vortex_host = 'sftp.captalys.io'
# vortex_username = 'vortx'
# vortex_private_key = "/home/ivan/Dev/glue_jobs/sftp/vortx_rsa.dat"
# port = 22
#
# brl_host = 'sftran.brltrust.com.br'
# brl_username = 'captalys'
# brl_private_key = "/home/ivan/Dev/glue_jobs/sftp/acessoBRL.pem"
# blr_password = "w@ferreira5"

if __name__ == '__main__':
    ...
# a = SftpConnector(brl_host, port, brl_username, brl_private_key)
# a.brl_conection(blr_password)
# a.vortx_conection()
## https://www.cloudskillsboost.google/users/sign_up?locale=pt_BR