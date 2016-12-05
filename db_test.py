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


def log(message=''):
    if message:
        print '[{}] {}'.format(datetime.now().strftime('%Y-%m-%d / %H:%M:%S'), message)
    else:
        print ''


def test_database():
    db = None
    test_started_at = datetime.now()

    log('Create temporary {} database on {}'.format(DB_NAME, DB_HOST))

    call('export PGPASSWORD={}'.format(DB_PASSWORD), shell=True)
    call('createdb -h {} -U{} {}'.format(DB_HOST, DB_USER, DB_NAME), shell=True)

    try:
        log('Connect to {}'.format(DB_NAME))

        db = records.Database('postgresql://{}:{}@{}:5432/{}'.format(DB_USER, DB_PASSWORD, DB_HOST, DB_NAME))

        log('Create table')

        db.query(
            'CREATE TABLE test ('
            'id BIGSERIAL PRIMARY KEY,'
            'title VARCHAR(50) NOT NULL,'
            'text CHAR(2000),'
            'floatvalue DECIMAL(10, 5));'
        )

        db.query('CREATE INDEX ON test ((lower(title)));')

        log('Insert {} rows with some random data'.format(ROWS_TO_INSERT))

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

        log('Select all rows')

        select_all_started_at = datetime.now()

        db.query('SELECT * FROM test;')

        select_all_finished_at = datetime.now()

        log('Select rows by id')

        select_by_id_started_at = datetime.now()

        for id in saved_ids:
            db.query('SELECT * FROM test WHERE id = {};'.format(id))

        select_by_id_finished_at = datetime.now()

        log('Select rows by title (indexed text)')

        select_by_title_started_at = datetime.now()

        for title in saved_titles:
            db.query('SELECT * FROM test WHERE title = \'{}\';'.format(title))

        select_by_title_finished_at = datetime.now()

        log('Select rows by text (non-indexed cutted text)')

        select_by_text_started_at = datetime.now()

        for text in saved_texts:
            db.query('SELECT * FROM test WHERE text LIKE \'{}%\';'.format(text))

        select_by_text_finished_at = datetime.now()

        log('Select rows by float value')

        select_by_float_started_at = datetime.now()

        for floatvalue in saved_floatvalues:
            db.query('SELECT * FROM test WHERE floatvalue = {};'.format(floatvalue))

        select_by_float_finished_at = datetime.now()

        log('Delete rows by title (indexed text)')

        delete_by_title_started_at = datetime.now()

        for floatvalue in saved_floatvalues:
            db.query('DELETE FROM test WHERE title = \'{}\';'.format(floatvalue))

        delete_by_title_finished_at = datetime.now()
        test_finished_at = datetime.now()

        log()
        log('Results:')
        log('   - Total time: {} seconds'.format(
            (test_finished_at - test_started_at).total_seconds()
        ))
        log('   - Insert operations: {} seconds'.format(
            (insert_finished_at - insert_started_at).total_seconds()
        ))
        log('   - Select all: {} seconds'.format(
            (select_all_finished_at - select_all_started_at).total_seconds()
        ))
        log('   - Select by id: {} seconds'.format(
            (select_by_id_finished_at - select_by_id_started_at).total_seconds()
        ))
        log('   - Select by title (indexed): {} seconds'.format(
            (select_by_title_finished_at - select_by_title_started_at).total_seconds()
        ))
        log('   - Select by text (non-indexed, cutted): {} seconds'.format(
            (select_by_text_finished_at - select_by_text_started_at).total_seconds()
        ))
        log('   - Select by float: {} seconds'.format(
            (select_by_float_finished_at - select_by_float_started_at).total_seconds()
        ))
        log('   - Delete rows by title: {} seconds'.format(
            (delete_by_title_finished_at - delete_by_title_started_at).total_seconds()
        ))
        log()
    finally:
        log('Drop temporary {} database from {}'.format(DB_NAME, DB_HOST))

        if db is not None:
            db.close()

        call('psql -h {} -U {} -c "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname=\'{}\';"'.format(
            DB_HOST, DB_USER, DB_NAME), shell=True, stdout=DEVNULL)

        call('dropdb -h {} {}'.format(DB_HOST, DB_NAME), shell=True)

        log('Done')
        log()


if __name__ == '__main__':
    test_database()
