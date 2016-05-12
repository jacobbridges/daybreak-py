from daybreak import DB
import os

file_path = './test.db'


def setup():
    return DB(file_path)


def cleanup(db):
    db.close()
    os.remove(file_path)


def test_daybreak_initializes():
    testdb = setup()
    assert testdb.closed() is False
    cleanup(testdb)


def test_daybreak_saves_record():
    testdb = setup()
    testdb['foo'] = 'bar'
    assert testdb['foo'] == 'bar'
    cleanup(testdb)


def test_daybreak_loads_record():
    testdb = setup()
    testdb['foo'] = 'bar'
    testdb.load()
    assert testdb['foo'] == 'bar'
    cleanup(testdb)


def test_daybreak_values_func():
    testdb = setup()
    testdb['foo'] = 'bar'
    testdb['baz'] = 'foo'
    assert testdb.values() == ['bar', 'foo']
    cleanup(testdb)
