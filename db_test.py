#! /usr/bin/env python

import records
import uuid
import random
import string
import sys
import os
from datetime import datetime
from subprocess import call

DEVNULL = open(os.devnull, 'wb')

DB_HOST = 'localhost'
DB_NAME = 'db_performance_test'
DB_USER = 'postgres'
DB_PASSWORD = 'postgres'
ROWS_TO_INSERT = int(sys.argv[1]) if len(sys.argv) > 1 else 10000


def now_str():
    return datetime.now().strftime('%Y-%m-%d / %H:%M:%S')


try:
    test_started_at = datetime.now()

    print '[{}] Create temporary {} database on {}'.format(now_str(), DB_NAME, DB_HOST)

    call('export PGPASSWORD={}'.format(DB_PASSWORD), shell=True)
    call('createdb -h {} -U{} {}'.format(DB_HOST, DB_USER, DB_NAME), shell=True)

    print '[{}] Connect to {}'.format(now_str(), DB_NAME)

    db = records.Database('postgresql://{}:{}@{}:5432/{}'.format(DB_USER, DB_PASSWORD, DB_HOST, DB_NAME))

    print '[{}] Create table'.format(now_str())

    db.query(
        'CREATE TABLE test ('
        'id BIGSERIAL PRIMARY KEY,'
        'title VARCHAR(50) NOT NULL,'
        'text CHAR(2000),'
        'floatvalue DECIMAL(10, 5));'
    )

    db.query('CREATE INDEX ON test ((lower(title)));')

    print '[{}] Insert {} rows with some random data'.format(now_str(), ROWS_TO_INSERT)

    insert_started_at = datetime.now()

    saved_ids, saved_titles, saved_floatvalues, saved_texts = [], [], [], []

    for i in xrange(ROWS_TO_INSERT):
        title, text = '', ''
        floatvalue = 0.0

        if not i % 100:
            saved_ids.append(i)
            title = uuid.uuid4()
            saved_titles.append(title)
            floatvalue = round(random.uniform(0, 1000), 5)
            saved_floatvalues.append(floatvalue)
            text = str(''.join([random.choice(string.ascii_letters) for _ in xrange(1000)]))
            saved_texts.append(text[:50])

        db.query('INSERT INTO test (title, text, floatvalue) VALUES (\'{}\', \'{}\', {});'.format(
                title or str(uuid.uuid4()),
                text or str(''.join([random.choice(string.ascii_letters) for _ in xrange(1000)])),
                floatvalue or round(random.uniform(0, 1000), 5)
            )
        )

    insert_finished_at = datetime.now()

    print '[{}] Select all rows'.format(now_str())

    select_all_started_at = datetime.now()

    rows = db.query('SELECT * FROM test;')

    select_all_finished_at = datetime.now()

    print '[{}] Select rows by id'.format(now_str())

    select_by_id_started_at = datetime.now()

    for id in saved_ids:
        rows = db.query('SELECT * FROM test WHERE id = {};'.format(id))

    select_by_id_finished_at = datetime.now()

    print '[{}] Select rows by title (indexed text)'.format(now_str())

    select_by_title_started_at = datetime.now()

    for title in saved_titles:
        rows = db.query('SELECT * FROM test WHERE title = \'{}\';'.format(title))

    select_by_title_finished_at = datetime.now()

    print '[{}] Select rows by text (non-indexed cutted text)'.format(now_str())

    select_by_text_started_at = datetime.now()

    for text in saved_texts:
        rows = db.query('SELECT * FROM test WHERE text LIKE \'{}%\';'.format(text))

    select_by_text_finished_at = datetime.now()

    print '[{}] Select rows by float value'.format(now_str())

    select_by_float_started_at = datetime.now()

    for floatvalue in saved_floatvalues:
        rows = db.query('SELECT * FROM test WHERE floatvalue = {};'.format(floatvalue))

    select_by_float_finished_at = datetime.now()

    print '[{}] Delete rows by title (indexed text)'.format(now_str())

    delete_by_title_started_at = datetime.now()

    for floatvalue in saved_floatvalues:
        rows = db.query('DELETE FROM test WHERE title = \'{}\';'.format(floatvalue))

    delete_by_title_finished_at = datetime.now()
    test_finished_at = datetime.now()

    print ''
    print '[{}] Results:'.format(now_str())
    print '[{}]   - Total time: {} seconds'.format(now_str(), (test_finished_at - test_started_at).total_seconds())
    print '[{}]   - Insert operations: {} seconds'.format(now_str(),
                                                          (insert_finished_at - insert_started_at).total_seconds())
    print '[{}]   - Select all: {} seconds'.format(now_str(),
                                                   (select_all_finished_at - select_all_started_at).total_seconds())
    print '[{}]   - Select by id: {} seconds'.format(
        now_str(), (select_by_id_finished_at - select_by_id_started_at).total_seconds()
    )
    print '[{}]   - Select by title (indexed): {} seconds'.format(
        now_str(), (select_by_title_finished_at - select_by_title_started_at).total_seconds()
    )
    print '[{}]   - Select by text (non-indexed, cutted): {} seconds'.format(
        now_str(), (select_by_text_finished_at - select_by_text_started_at).total_seconds()
    )
    print '[{}]   - Select by float: {} seconds'.format(
        now_str(), (select_by_float_finished_at - select_by_float_started_at).total_seconds()
    )
    print '[{}]   - Delete rows by title: {} seconds'.format(
        now_str(), (delete_by_title_finished_at - delete_by_title_started_at).total_seconds()
    )
    print ''
finally:
    print '[{}] Drop temporary {} database from {}'.format(now_str(), DB_NAME, DB_HOST)
    db.close()

    call('psql -h {} -U {} -c "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname=\'{}\';"'.format(
        DB_HOST, DB_USER, DB_NAME), shell=True, stdout=DEVNULL)

    call('dropdb -h {} {}'.format(DB_HOST, DB_NAME), shell=True)
    print '[{}] Done'.format(now_str())
    print ''
