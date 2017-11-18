from elasticsearch import Connection
from random import randint

BASEURL = 'http://127.0.0.1:9200'


def test_create_with_timeout():
    index = 'test_create_' + str(randint(1000, 9999))
    con = Connection(BASEURL)
    settings = {'index': {'number_of_shards': 4, 'number_of_replicas': 0}}
    result = con.index_create(index, settings=settings, timeout='1m')

    assert result is True

    con.index_delete(index)


def test_create_default():
    index = 'test_create_' + str(randint(1000, 9999))
    con = Connection(BASEURL)
    result = con.index_create(index)

    assert result is True

    con.index_delete(index)
