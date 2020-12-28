"""
Задачи данного процесса:
1) считать из S3 файлы с RSS-потоком
2) распарсить и выделить в них именованные сущности
3) записать результат в stage-таблицы БД
4) переместить файлы в другой бакет
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
    Функция, которая выделяет из текста NER
    Формат возвращаемых значений - список из кортежей ('Наименование','Тип')
    Тип согласно документации к модулю natasha
    :param text: текст для анализа
    :return: список уникальных кортежей (наименование сущности, тип сущности)
    """

    ners = set()
    doc = Doc(text.strip())  # обрезка пробелов, чтобы natasha не падале с ошибкой

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
    Функция, записывающая данные в БД
    :param row_list: список кортежей для записи в БД
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
    Функция, обработки файлов: читает данные из бакета, вызывает text_analyze для выделения NER,
    затем преобразуюет результаты анализа в список кортежей и вставляющет их в БД
    Вставка в БД производится для каждого файла.
    :param rss_bucket: имя бакета с файлами
    :return: processed_file_list: список обработанных файлов
    """
    processed_file_list = []

    for file in rss_bucket.objects.all():
        news_src = file.key[-7:-4]
        processed_file_list.append(file.key)
        data_list = []
        xml_tree = ET.fromstring(file.get()['Body'].read())
        print(f'Processing file {file.key}...')
        cnt = 0  # счетчик для логирования

        for element in xml_tree.findall("channel/item"):
            guid = element.find("guid")
            pub_date = element.find("pubDate")
            description = element.find("description")
            cnt += 1
            try:    # Иногда в RSS-лентах есь ошибки в формате :(
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
    Функция для перемещения обработанных файлов в бакет с архивными файлами
    :param file_list: список файлов для перемещения в архив
    :param s3: объект boto3.resource
    :return: nothing
    """

    cnt = 0  # счетчик для логирования
    for file_name in file_list:
        src = {'Bucket': S3_BUCKET, 'Key': file_name}
        s3.meta.client.copy(src, S3_ARCHIVE_BUCKET, file_name)
        s3.Object(S3_BUCKET, file_name).delete()
        cnt += 1
    print(f'{cnt} files have been moved')


def delete_files(s3, bucket_name, file_list):
    """
    Функция удаления обработанных файлов
    :param s3: объект boto3.resource
    :param bucket_name: бакет, из которого надо удалить файлы
    :param file_list: список файлов к удалению
    :return: nothing
    """

    cnt = 0  # счетчик для логирования
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
