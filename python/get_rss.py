"""
Процедура, для загрузки RSS-ленты в виде XML-файла на S3
Запускается из командной строки, на вход принимает два параметра:
    - адрес RSS-канада
    - код источника канала
"""
from cfg_settings import AWS_ACCESS_KEY, AWS_SECRET_KEY, S3_BUCKET, REGION_NAME
import boto3
import requests
import time
from sys import argv


def uploader(url, owner):
    """
    Функция, для загрузки RSS-ленты в виде XML-файла на S3
    :param url: адрес RSS-канала
    :param owner: код источника канала (используется для формирования постфикса имени XML-файла
    :return: nothing
    """
    s3 = boto3.resource(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
        verify=False,
        region_name=REGION_NAME
    )

    r = requests.get(url)
    file_name = str(int(time.time())) + "_" + owner + ".xml"
    s3.Object(S3_BUCKET, file_name).put(Body=r.text)
    print(f'The file {file_name} has uploaded')


if __name__ == "__main__":
    _, url, owner = argv
    uploader(url, owner)