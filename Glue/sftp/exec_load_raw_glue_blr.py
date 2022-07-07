import boto3



aws_access_key_id="ASIAS34UIATUPTIZ3DF3"
aws_secret_access_key="E8ECtFLiID4My82CcZjFq00wmscuUwD+Z3qLX+4v"
aws_session_token="IQoJb3JpZ2luX2VjEJb//////////wEaCXNhLWVhc3QtMSJGMEQCIC71FKudgmJ0+3j+VH3qbsXXTUZ/BYXlrHX12Vhk/8MtAiAkhIwZOsKx69bgh7QBTGGvm7upl8zKXXLESod4/SPR0SqrAwi///////////8BEAEaDDE5NzM0MjUyODc0NCIM5IDO9JvbzyUSqw5WKv8CuUwD1M1M/kdysyPe/auVD8sRqf0KQAySVvfDxBlYBCh1kIdVZfXeNLMFnIfQV+XxP813MzULVWW3WO4swWRMY9W7ONwjCCuuFKbaFW/iC319PeX3C0NBQXvPrF3KzYmyiVFsT5SAt77yXDWKKHkh2NctYnHOhgw8jmSEk1yUvTypbpqZynz+15QsIc/0Wl0goGpZgQnSrOiXSrbS91nuSWew/mFRwsebrxgHG1jRDjmlvcYiJclcW/h+pIAjl0txRCuB79/ylaBGzfiGSZMfI9OEPUq3wyUdnoEQXJ3ncZKgrqWlLLg80M6Gv6YmDt3KDllbeFeLBLBZoQeKdr9vn/SYAiGdZ3kVkdwhnH3Y9YqX1ntzZaUsGwfdDDvQR+LovAzegqz1D3fv8LKyNrYwBwR6me3EKn6qpfWT+CA7Qv/C7/9pY6cBBLhRDyfFdC6z44hi2upTdxfqZcshsSX/EIcnlR/pf/xtemxytVVxBMH/EVnif0wI390YKUtAbwww1p2WlgY6pwF/L1KNI7fssVwMv4tAdVTmVuYU91+ps6niPbGjzx6bX7k6q1y7icHf99dCqWQsfRvZ+8mt7S2g29bw6a4ACcm0QBf+yiC81DpIxb072LYEOZ2n9a76QQuiQzrS5tUaZpY9n9dZ2xG8ApXEiMD2eYWQiRNvBYU3qDZBfK8MTOwXcWsfa6Fjmvg70mFUF7Yd7eDSLg2rXVmntUH872G+k62+FFem3igtFA=="

glue_client = boto3.client(
    "glue",
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    aws_session_token=aws_session_token,
    region_name='sa-east-1'
)

listaGeral = [
    '2022-07-04'

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
