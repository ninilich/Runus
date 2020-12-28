from datetime import datetime


FEEDS_LIST = [
    ("https://lenta.ru/rss/", "LNT", "0 4 * * *"),
    ("https://echo.msk.ru/news.rss", "EHO", "20 1-23/2 * * *"),
    ("https://ria.ru/export/rss2/index.xml", "RIA", "25,55 * * * *"),
    ("http://static.feed.rbc.ru/rbc/internal/rss.rbc.ru/rbc.ru/newsline.rss", "RBK", "05 4,16 * * *"),
    ('https://news.yandex.ru/politics.rss', "YPL", "10 4,16 * * *"),
    ('https://news.yandex.ru/business.rss', "YEC", "15 4,16 * * *")
]

START_DATE = datetime(2020, 12, 21)