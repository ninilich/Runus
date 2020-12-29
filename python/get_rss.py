"""
Procedure for uploading an RSS-feed as an XML-file to S3
Runs from the command line, accepts two parameters as input:
    - address of the RSS-feed
    - the source code of the channel
"""
from cfg_settings import AWS_ACCESS_KEY, AWS_SECRET_KEY, S3_BUCKET, REGION_NAME
import boto3
import requests
import time
from sys import argv


def uploader(url, owner):
    """
    Function for uploading an RSS feed as an XML file to S3
    :param url: RSS-feed url
    :param owner: channel source code (used to form the postfix of the XML-file name)
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