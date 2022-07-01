from datetime import date, timedelta
import time
import boto3

aws_access_key_id="ASIAS34UIATUPI4L7CPI"
aws_secret_access_key="7slgruf3zUWo6pOPjG2I38c4nTdgZUJoqJUwNuA/"
aws_session_token="IQoJb3JpZ2luX2VjEAYaCXNhLWVhc3QtMSJGMEQCIGAgeaAv2rpRfmjnKKiS2818lqaCgWjuNs4IWPyr33wMAiA2b/LO2Y+K2CEEWt2vcLkx9Mq9HMhAL2Hf2lTsw2EHoSqiAwgvEAEaDDE5NzM0MjUyODc0NCIMFQf1zDIPPuWzFBIgKv8CNwyk94jWGWLaKzyARXSjHSkJ1W/ApLnvn/Zy4LsJjDhVweMM+Y3OhsygoisiSNa2U/QDR+lSKGcu6o9RqF4obNhAEWVa6l16j83dvfAF7PK5l39S6JsEwT5OY12pbCXq1z91mjadKQv8GN7Yw3k9NC3OLV2VZG67N8tdAVisCmwRIv1O3bfcJ8r4abx+eCCukWgbhhmLsoDYkVNWPmnkuaHcpI9T7Ib/J7gjcxIDLxdkAoyL8i4N6gad9bvPYhvk1+tpxY2k5aNDPiC5gCy3GHBp5AiP03YdrUjJIvF0ntGyjFSJJbytJje+YvE1oYfO8cRj4daHXSPt1Vg5ly35Y8si5ZYcfLhQ6ybkzv3RR3mzSAOMPc4RuWutNvUYfFdTv52KtLz7WZJbVUVQl1WUDyzwoLtlv1nVY/bEHgJ3hc1sCo6hxqhoV/RjLt5aanmfFkl9WGQou5P147lCaqRORXTVUw9EJdWSb/p4xEbj14lUQNFDFv4jDny+mcHlixAw59D2lQY6pwEJqrlTc+3YiEj/+RlbkMg44inwY1bnf0bUlqj+Xa5iHZcnoyk4aJ3+6/n56RTiJlwR7mG39DBPXZQxAA443tIu4vKbsXaUW2RDFNwfUcmngMWnOCXGB1k5vYokNgT5e0yI4fXlsT2L4rt3stWPnweBYHCHPlGRmOca0Ivr20o+lPTpdcSmO8YX+HfDEvNJtPy0E0N1gmd0PJ0806/OpuvhwOwvHM0nGQ=="

glue_client = boto3.client(
    "glue",
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    aws_session_token=aws_session_token,
    region_name='sa-east-1'
)

listaGeral = [
    '2022-06-28',
    '2022-06-29'

]

if __name__ == "__main__":

    for i in listaGeral:
        glue_args = {
            'class': 'GlueApp',
            'date': i,
            'domain': 'brl',
            'file': 'aquisicao',
            'land_bucket': 'captalys-analytics-land-production',
            'source_bucket': 'captalys-analytics-raw-production',
            'technology': 'sftp'
        }

        args = {
            '--class': glue_args['class'],
            '--date': glue_args['date'],
            '--domain': glue_args['domain'],
            '--file': glue_args['file'],
            '--land_bucket': glue_args['land_bucket'],
            '--source_bucket': glue_args['source_bucket'],
            '--technology': glue_args['technology']
        }

        print(args)

        glue_client.start_job_run(
            JobName='datalake-trusted-brl',
            Arguments=args)
