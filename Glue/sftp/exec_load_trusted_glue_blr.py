from datetime import date, timedelta
import time
import boto3

aws_access_key_id = "ASIAS34UIATUAZRP4JJP"
aws_secret_access_key = "yr0zJXgu4zx9yZrb/6cX/J0GWFyc6gDebHqwY0Aa"
aws_session_token = "IQoJb3JpZ2luX2VjEGYaCXNhLWVhc3QtMSJGMEQCICASryxV81eMQoQ8uGCt6Zi/oC78zHgO+dHasH5uap7MAiA8aF3VZfnfCI5pIMGZtdHsnLRfISRCUf6cCQIzFyc6LCqrAwiP//////////8BEAEaDDE5NzM0MjUyODc0NCIMQAuraDLUGdfjU8PYKv8CKOQekyHrhkkr09Wg9kcR6Hf15Mc1Vv8Vmm0ga4KxTuGi/F2IwB3+OPm6jJv3Z294MeTJWEhrrFsdKdvwDkuRvNk5lzkrPu71ZT/LT4WmWEWKlomtXvKkkeoibmpmvPo9NalY8DyORwXP2+0zQVj6z+xhq8oZRGY2mb+qMxZuGNwLbj3mM902V2BCTx84M7VB2ALSmXv+zdTSbgTwxGr9xjIOmekRfQAiw+EXH7KFeufLHGzJkAaTSHSDhenBuC6XLH9eeoKm/9zlwwmIHfXvzsaTtN6IPwC1LKeJ0PZ5Psrjy9ymM4lVO2sQ38jGA0InC1kD5eNrkTUhaDjAwELEXegHZOlO5B9iH0ByzKrSjQcwK9RkiZJOU5YbtFUT1fofN7+Ys6JqNo4xDt4t5nYWlXExfS8DbiXFn6As34DIR2JpWGqfFscUf/ywkQ+EYDsPKiOLDk640qQonwyq/V6ZwuUFQfCphEkdiyt+97jaRklmm2eGON7vcZqjG4vCxhYw6NiLlgY6pwFgIq5oDF5CgKrx9k8VNHGwZn5h4OVrcpgPv+OghmnQ13YnvVpbRvNBt+l9uZVzA+CwPpVqF0MZK5voD+yYjLJ4ZZV+C6Ixdd2DXnSAjjKZLhIjBn7vjEavjBEYB6FryEH12vRtxoOhHH/36FBy4kjOCkF4Q/BJ+DLitX/ht26tAqw+anCHU7D7oD1BqBDY+zn5Ivja36NnicJNTP9uSMcVt0n0VpeR1A=="

glue_client = boto3.client(
    "glue",
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    aws_session_token=aws_session_token,
    region_name='sa-east-1'
)

listaGeral = [
    '2022-06-30',
    '2022-07-01'

]

if __name__ == "__main__":

    for i in listaGeral:
        glue_args = {
            'class': 'GlueApp',
            'date': i,
            'domain': 'brl',
            'file': 'estoque',
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
            #JobName='datalake-trusted-vortx',
            Arguments=args)
