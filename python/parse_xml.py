"""
This py-file do:
1) reads xml-files with RSS-feeds from S3-bucket
2) analyzes these files and detects Named Entities
3) saves result into DB
4) deletes processed files
"""

import psycopg2 as pg
import boto3
import xml.etree.ElementTree as ET

from dateutil.parser import parse
from natasha import (Segmenter, MorphVocab,
                     NewsEmbedding, NewsMorphTagger, NewsSyntaxParser, NewsNERTagger, NamesExtractor,
                     Doc
                     )
from cfg_settings import (AWS_ACCESS_KEY, AWS_SECRET_KEY, S3_BUCKET, REGION_NAME,
                          DB_HOST, DB_NAME, DB_USER, DB_PASSWORD, S3_ARCHIVE_BUCKET)


def text_analyze(text):
    """
    Function for detecting named entities from the text
    :param text: text to analyze and to detect Named Entities
    :return: list of unique tuples (entity_name, entity_type)
    """

    ners = set()
    doc = Doc(text.strip())  # trim spaces so that "natasha" doesn't fall with an error

    # After applying segmenter two new fields appear: sents and tokens
    doc.segment(segmenter)

    # After applying morph_tagger and syntax_parser, tokens get 5 new fields id, pos, feats, head_id, rel
    doc.tag_morph(morph_tagger)
    doc.parse_syntax(syntax_parser)

    # After applying ner_tagger doc gets spans field with PER, LOC, ORG annotation
    doc.tag_ner(ner_tagger)

    # Normalizing
    for span in doc.spans:
        span.normalize(morph_vocab)

    # Extracting
    for span in doc.spans:
        span.extract_fact(names_extractor)
    for item in doc.spans:
        ners.add((item.normal, item.type))

    return list(ners)


def insert_db(row_list):
    """
    Function for saving data into DB
    :param row_list: list of tuples for saving in DB
    :return: nothing
    """
    print('Starting to insert data into database')
    with pg.connect(host=DB_HOST, user=DB_USER, password=DB_PASSWORD, dbname=DB_NAME) as connection:
        with connection.cursor() as cursor:
            table_name = 'public.stg_news'
            query = "INSERT INTO " + table_name + "(news_src, news_datetime, entity_nm, entity_type, news_uid) " \
                                                           "VALUES (%s,%s,%s,%s,%s) "
            cursor.executemany(query, row_list)
            connection.commit()
    print('Data inserted into database')

def proc_files(rss_bucket):
    """
    File processing function: reads data from the bucket, calls test_analyze to detect Names Entities,
    then it converts the analysis results into a list of tuples and inserts them into the database.
    Insertion into the database is performed for each file.
    :param rss_bucket: S3-backer name with files
    :return: processed_file_list: list of names of processed files
    """
    processed_file_list = []

    for file in rss_bucket.objects.all():
        news_src = file.key[-7:-4]
        processed_file_list.append(file.key)
        data_list = []
        xml_tree = ET.fromstring(file.get()['Body'].read())
        print(f'Processing file {file.key}...')
        cnt = 0  # for logging

        for element in xml_tree.findall("channel/item"):
            guid = element.find("guid")
            pub_date = element.find("pubDate")
            description = element.find("description")
            cnt += 1
            try:    # Sometimes RSS feeds have errors in the format :(
                for item in text_analyze(description.text):
                    entity_nm, entity_type = item
                    data_item = (news_src, parse(pub_date.text), entity_nm, entity_type, guid.text)
                    data_list.append(data_item)
            except:
                print (f'ERROR processing file {file}, text = {description.text}')
        insert_db(data_list)
        print(f'{cnt} news was processed')

    return processed_file_list


def archive_files(s3, file_list):
    """
    This function will move the processed files to the package with the archive file
    Used during debugging, not used now
    :param file_list: list of file to move
    :param s3: object boto3.resource
    :return: nothing
    """

    cnt = 0  # for logging
    for file_name in file_list:
        src = {'Bucket': S3_BUCKET, 'Key': file_name}
        s3.meta.client.copy(src, S3_ARCHIVE_BUCKET, file_name)
        s3.Object(S3_BUCKET, file_name).delete()
        cnt += 1
    print(f'{cnt} files have been moved')


def delete_files(s3, bucket_name, file_list):
    """
    Function for deleting processed files
    :param s3: object boto3.resource
    :param bucket_name: S3-bucket, from which files will be deleted
    :param file_list: list of filenames to delete
    :return: nothing
    """

    cnt = 0  # for logging
    for file_name in file_list:
        s3.Object(bucket_name, file_name).delete()
        cnt += 1
    print(f'{cnt} files have been deleted')

if __name__ == "__main__":
    s3 = boto3.resource(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
        verify=False,
        region_name=REGION_NAME
    )
    rss_bucket = s3.Bucket(S3_BUCKET)

    # Objects for natasha
    segmenter = Segmenter()
    morph_vocab = MorphVocab()
    emb = NewsEmbedding()
    morph_tagger = NewsMorphTagger(emb)
    syntax_parser = NewsSyntaxParser(emb)
    ner_tagger = NewsNERTagger(emb)
    names_extractor = NamesExtractor(morph_vocab)

    # main processing
    processed_file_list = proc_files(rss_bucket)
    delete_files(s3, S3_BUCKET, processed_file_list)
    # archive_files(s3, processed_file_list)
