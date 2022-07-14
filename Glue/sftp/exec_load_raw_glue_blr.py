import boto3


aws_access_key_id="ASIAS34UIATUJCYIMXBE"
aws_secret_access_key="CMZlL9izFdy90if1WlnvYOpFPjDkzWCVGD02elfO"
aws_session_token="IQoJb3JpZ2luX2VjED8aCXNhLWVhc3QtMSJGMEQCIHpH2YHnvbNEi8QtS0h4PjskrwJpF6RQTSdEyCMFCdW5AiBgT1eFHXsOJIqF1stqMDHaZJEb/JkmhE8ZeJ2MHDVMNiqiAwh4EAEaDDE5NzM0MjUyODc0NCIMLF9FvS/THgLMjPMQKv8CUzj24GZagZO5sVNwHLKrzwyzzLgbWrxExva8iE43+TTWlDykSQ8F+vnklrhVBPK7Ps4M/ONgzxG8UDdEvzglFrenUD+lxV1xi4v8qg/dRkA8pvIziJY5f2muWsbTIUwBCjH3MXG8Yew/6G7DvEqMtMSyrGLsPhrD6x98/Ls/GNL7igVHf9sW1W1Ore/veOGhnBn/YHadsj8o/W2x5OCaPIxfePoQCm3TbwC/Xf+5n9WGEB+YIWzApLfrSRENRNmdEHeUzvc1YsBgySOIax5XIPwVsgcdGt3kh7S3WkWki/L/Qhd83CYUvOiEf/keRtLFOSHlx2COZpsQm+ZW8jO9iHrIaNQnbWlFNNnTtTTQi+DLCp9XAopkkf8YILU/JT00EtiWhJXSOYWEh8qJ46GW65do75UYgCItasSs4BvbWMq8I5w3QuECFTPqzOQwIVwvMWgVAcsOVvFz6zQfi5jOC5YnP8a3b++DNN6Z0Ak7+cCefgkSyOs0dpZZ17ASpaYw1MC7lgY6pwEU0xYCqOe3ahcBKlBr+zqyTaTzSc+bzELxu4UMPxCrdWL7FxUs2uWFvKKz203SOqYqipMhdYeSDoJJzbhs8gKCnfsfDsLf0b6d5i7XmgTbFH8hos2mjAqGrAIlzzYSPrIh/eVjiTnMrzPuNclRb9x3RuCv49845fSwCr1VO/mnEcubqGvic3cGDK4OXJ7QpVda1kf+4dZt1k7RZmSDlA25saOBYcfcGw=="
glue_client = boto3.client(
    "glue",
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    aws_session_token=aws_session_token,
    region_name='sa-east-1'
)

M = '06'
listaGeral = [

    f'2022-{M}-01',
    f'2022-{M}-02',
    f'2022-{M}-03',
    f'2022-{M}-06',
    f'2022-{M}-07',
    f'2022-{M}-08',
    f'2022-{M}-09',
    f'2022-{M}-10',
    f'2022-{M}-13',
    f'2022-{M}-14',
    f'2022-{M}-15',
    f'2022-{M}-17',
    f'2022-{M}-20',
    f'2022-{M}-21',
    f'2022-{M}-22',
    f'2022-{M}-23',
    f'2022-{M}-24',
    f'2022-{M}-27',
    f'2022-{M}-28',
    f'2022-{M}-29',
    f'2022-{M}-30',
    f'2022-07-01',
    f'2022-07-04',
    f'2022-07-05',
    f'2022-07-06',
    f'2022-07-11',

]

if __name__ == "__main__":

    for i in listaGeral:
        glue_args = {
            'class': 'GlueApp',
            'date': i,
            'destination_bucket': 'captalys-analytics-raw-production',
            'domain': 'singulare',
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
            #JobName='datalake-raw-brl',
            #JobName='datalake-raw-vortx',
            JobName='datalake-land_to_raw-singulare',
            Arguments=args)


