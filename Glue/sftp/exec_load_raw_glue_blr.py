import boto3


aws_access_key_id="ASIAS34UIATUAIPHGAVD"
aws_secret_access_key="Mwfk25BulF1YniDsxmyafJ0ZPP8ajyA5A1uJoz9x"
aws_session_token="IQoJb3JpZ2luX2VjECMaCXNhLWVhc3QtMSJHMEUCIBf8i9VBuvJxGUISrtgOhtlHsLqpy/of2GPewMGDh2FvAiEA08XG23m1mxEntmk0VA2v69aiaKPP6Vo1qLowlj0ViHQqogMITBABGgwxOTczNDI1Mjg3NDQiDDjkEclQMlZ81uQy+Sr/AmQ+1DDFkb+OQim1XyLsD3MjNF4H9MK0yZ2ZJWbGAZYujag69b/kGRltQ9IPyzTPyx4shL/D5/IqCY7XSAEUWV0FBvFEb8dVwTCBy3zJH34t3Ej2MrMswZgC2UWO+oZcF5F25DlD+FdybBdtwHH3loIOoXNKYi6Oxg/Hv6oyWgwRbESHD66ofddJXhDql3N6bGAGfAxtNJNJSPWhFRDUhXz48Q37RKs5hVteh23OQMRdOoGoyLcM9GOaw+JIdCAcrjBFXOw4xJgXo6BDLCFC67ONbz/VaaBD7Q7LCPcbB/EFo9Cun+fU7h6+C8UDlXQKW1lQzRKspn7MSOgqAn8MGOBxrAC64tKDTDih3WTERovtO2W0Ootar1AZj4HKwc4Cr+QcEv3f0rstzdZh/jIk8hd06O+tD6f8SKCj7RC9pHyMFZ/kQRd/sX8Gwtm/VB2qtnqZyfT/CkzX/TNHZW3R1K2qY1eHef+1wbVxPsr7PWWS9Ybeipfgj6kC26bMG1BAMM34/JUGOqYBRbFFno3ekrRJuhegTEw7jbno1HvWWmq3yLq0cjiyeXKL5KqGJj1OGQrher+yYMhAP81F5DTW87FVbijKDlw6Nl9O7uQbRfZhWV2WxC8AQHrqsn8Rg0KjrBGh/k08dL6xXenfcJperP5RmvtNHHdy97YcFSrqeJ1K5KcPv/TTW/y1Feq/pFFWXj9jvF1LyHsk6ioScngqJ/RlJeeiJbgWKMYFTFGnLQ=="
glue_client = boto3.client(
    "glue",
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    aws_session_token=aws_session_token,
    region_name='sa-east-1'
)

listaGeral = [
    '2022-06-25',
    '2022-06-26',
    '2022-06-27',
    '2022-06-28',
    '2022-06-29',

]

if __name__ == "__main__":

    for i in listaGeral:
        glue_args = {
            'class': 'GlueApp',
            'date': i,
            'destination_bucket': 'captalys-analytics-raw-production',
            'domain': 'brl',
            'file': 'estoque',
            'source_bucket': 'captalys-analytics-land-production',
            'technology': 'sftp'
        }

        args = {
            '--class': glue_args['class'],
            '--date': glue_args['date'],
            '--destination_bucket': glue_args['destination_bucket'],
            '--domain': glue_args['domain'],
            '--file': glue_args['file'],
            '--source_bucket': glue_args['source_bucket'],
            '--technology': glue_args['technology']
        }

        print(args)

        glue_client.start_job_run(
            JobName='datalake-raw-brl',
            #JobName='datalake-raw-vortx',
            Arguments=args)
