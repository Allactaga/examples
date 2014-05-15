# -*- coding: utf-8 -*-
"""
This is an example of query-based model construction approach for PostgreSQL database.

Attributes of models here do not depend on tables columns or even the existence of tables. As shown below,
models can be constructed from any query. This gives developer the full power of database, not limited to
subset of ORM features. Just remember that with great power comes great responsibility. )

This script requires Python 2/3 with installed psycopg2 package to run.
"""

import sys
from collections import namedtuple
from pprint import pprint

from psycopg2 import connect

DB_LOGIN = 'user'
DB_PASSWORD = 'password'
DB_NAME = 'database'
DB_HOST = 'localhost'
DB_PORT = 5432


class Object(object):
    """ Common base class for all database models.
    If primary_key attribute is set, then constructed instances are the same for identical primary keys. """

    _instance_map = None
    primary_key = None

    is_admin = False

    def __new__(cls, **kwargs):
        if cls.primary_key is None:
            return super(Object, cls).__new__(cls)

        if cls._instance_map is None:
            cls._instance_map = {}

        cache_key = tuple(
            kwargs[column]
            for column in cls.primary_key
        )

        if cache_key in cls._instance_map:
            obj = cls._instance_map[cache_key]
        else:
            obj = super(Object, cls).__new__(cls)
            cls._instance_map[cache_key] = obj

        return obj

    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)

    def update(self, **kwargs):
        self.__dict__.update(kwargs)


class Query(object):
    """ Query container. """

    def __init__(self, query_string, *args, **kwargs):
        if args and kwargs:
            raise ValueError('Both named and unnamed arguments passed to query: {}'.format(query_string))
        self.query_string = query_string
        if kwargs:
            self.args = [kwargs]
        else:
            self.args = args

    def __repr__(self):
        if len(self.args) == 1 and isinstance(self.args[0], dict):
            query_args = {
                name: repr(value)
                for name, value in self.args[0].items()
            }
        else:
            query_args = [
                repr(value)
                for value in self.args
            ]

        return self.query_string % query_args


class Singleton(object):
    """ Singleton pattern class """

    _instance = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(Singleton, cls).__new__(cls)
        return cls._instance


class Database(Singleton):
    """ This class is responsible for interactions with database.
    `initialize` method should be called before executing queries. """
    database = None
    user = None

    _connection = None

    def initialize(self, database, user, password, host=None, port=None):
        if self._connection is not None:
            raise RuntimeError('Database connection already exists')

        self._connection = connect(
            database=database,
            user=user,
            password=password,
            host=host,
            port=port,
        )

        self.database = database
        self.user = user

    def commit(self):
        self._get_connection().commit()

    def rollback(self):
        if self._connection is not None:
            self._connection.rollback()

    def execute(self, query):
        return self._get_cursor_for_query(query)

    def get_one(self, query):
        cursor = self._get_cursor_for_query(query)
        row = cursor.fetchone()

        if row is None:
            return row

        return self._populate_rows_with_names(
            rows=[row],
            names=self._get_column_names_from_cursor(cursor)
        )[0]

    def get_all(self, query):
        cursor = self._get_cursor_for_query(query)

        return self._populate_rows_with_names(
            rows=cursor.fetchall(),
            names=self._get_column_names_from_cursor(cursor)
        )

    def _get_connection(self):
        if self._connection is None:
            raise RuntimeError('No database connection')

        return self._connection

    def _get_cursor_for_query(self, query):
        cursor = self._get_connection().cursor()
        cursor.execute(query.query_string, *query.args)
        return cursor

    def _get_column_names_from_cursor(self, cursor):
        return [column.name for column in cursor.description]

    def _populate_rows_with_names(self, rows, names):
        named_rows = []
        for row in rows:
            named_rows.append({name: value
                               for name, value in zip(names, row)})
        return named_rows


class Options(Object):
    """ This is an example model of key-value storage. """
    primary_key = ('name',)

    @classmethod
    def get_all_options(cls):
        options_info = Database().get_all(
            Query('SELECT * FROM options ORDER BY LOWER(name)')
        )
        return [cls(**option_info) for option_info in options_info]

    @classmethod
    def get_option(cls, name):
        option_info = Database().get_one(
            Query('SELECT * FROM options WHERE name = %(name)s', name=name)
        )
        if option_info:
            return cls(**option_info)

    @classmethod
    def add_option(cls, name, value):
        option_info = Database().get_one(
            Query(
                'INSERT INTO options (name, value) VALUES (%(name)s, %(value)s) RETURNING *',
                name=name,
                value=value,
            )
        )
        return cls(**option_info)

    def update(self, value):
        option_info = Database().get_one(
            Query(
                'UPDATE options SET value = %(value)s WHERE name = %(name)s RETURNING *',
                name=self.name,
                value=value,
            )
        )
        super(Options, self).update(**option_info)

    @classmethod
    def create_demo_table(cls):
        queries = [
            Query('CREATE TABLE options (name TEXT NOT NULL PRIMARY KEY, value TEXT NOT NULL)'),
            Query('INSERT INTO options (name, value) VALUES (%(name)s, %(value)s)', name='first', value='one'),
            Query('INSERT INTO options (name, value) VALUES (%(name)s, %(value)s)', name='second', value='two'),
            Query('INSERT INTO options (name, value) VALUES (%(name)s, %(value)s)', name='third', value='three'),
            Query('INSERT INTO options (name, value) VALUES (%(name)s, %(value)s)', name='forth', value='four'),
        ]
        for query in queries:
            Database().execute(query)

    @classmethod
    def destroy_demo_table(cls):
        Database().execute(Query('DROP TABLE options'))

    def __repr__(self):
        return '<Option: name={name!r}, value={value!r}>'.format(
            name=self.name,
            value=self.value,
        )


class UserTablesStats(Object):
    """ Example model of user table usage statistics. """

    @classmethod
    def get_stats(cls):
        stats_info = Database().get_all(
            Query('SELECT schemaname AS schema, relname AS table,'
                  ' seq_scan, idx_scan, now() as timestamp FROM pg_stat_user_tables')
        )
        return [cls(**table_stats) for table_stats in stats_info]

    def __repr__(self):
        return (
            '<UserTablesStats: schema={schema!r}, table={table!r},'
            ' sequential_scans={seq_scan!r}, index_scans={idx_scan!r}, timestamp={timestamp!r}>'.format(
                schema=self.schema,
                table=self.table,
                seq_scan=self.seq_scan,
                idx_scan=self.idx_scan,
                timestamp=self.timestamp.strftime('%Y/%m/%d %H:%M:%S'),
            ))


Point = namedtuple('Point', ['x', 'y'])


class RandomCoordinates(Object):
    """ Example model which does not require table. """

    primary_key = ('x', 'y')

    @classmethod
    def pick(cls, point1, point2):
        min_x = min(point1.x, point2.x)
        min_y = min(point1.y, point2.y)
        max_x = max(point1.x, point2.x)
        max_y = max(point1.y, point2.y)

        coordinates_info = Database().get_one(
            Query(
                'SELECT'
                ' (RANDOM() * %(width)s + %(min_x)s)::int AS x,'
                ' (RANDOM() * %(height)s + %(min_y)s)::int AS y;',
                width=max_x - min_x,
                height=max_y - min_y,
                min_x=min_x,
                min_y=min_y
            )
        )

        return cls(**coordinates_info)

    def __repr__(self):
        return '<RandomCoordinates: x={x!r}, y={y!r}>'.format(x=self.x, y=self.y)


def main(argv=None):
    if argv is None:
        argv = sys.argv

    # Connect to a database
    Database().initialize(user=DB_LOGIN, password=DB_PASSWORD, database=DB_NAME, host=DB_HOST, port=DB_PORT)

    # Create and populate key-value storage
    Options.create_demo_table()

    # Get an existing option
    first_option = Options.get_option('first')
    print(first_option)

    # Create new option
    fifth_option = Options.add_option(name='fifth', value='five')
    print(fifth_option)

    # Update existing option
    first_option.update(value='The One')
    print(first_option)

    # List all available options
    option = Options.get_all_options()
    pprint(option)

    # Get user table usage statistics from PostgreSQL predefined view
    stats = UserTablesStats.get_stats()
    pprint(stats)

    # Generate 5 random coordinates
    coordinates = [
        RandomCoordinates.pick(Point(x=-500, y=-500),
                               Point(x=500, y=500))
        for _ in range(5)
    ]
    pprint(coordinates)

    # Drop previously created table
    Options.destroy_demo_table()


if __name__ == '__main__':
    try:
        sys.exit(main())
    finally:
        Database().rollback()
