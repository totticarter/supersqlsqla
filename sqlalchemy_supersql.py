"""Integration between SQLAlchemy and Presto.

Some code based on
https://github.com/zzzeek/sqlalchemy/blob/rel_0_5/lib/sqlalchemy/databases/sqlite.py
which is released under the MIT license.
"""

from __future__ import absolute_import
from __future__ import unicode_literals
from distutils.version import StrictVersion
from PySupersql import supersql
from PySupersql.common import UniversalSet
from sqlalchemy import exc
from sqlalchemy import types
from sqlalchemy import util
from sqlalchemy.engine import default
from sqlalchemy.sql import compiler
import re
import sqlalchemy

try:
    from sqlalchemy.sql.compiler import SQLCompiler
except ImportError:
    from sqlalchemy.sql.compiler import DefaultCompiler as SQLCompiler


class SupersqlIdentifierPreparer(compiler.IdentifierPreparer):
    # Just quote everything to make things simpler / easier to upgrade
    reserved_words = UniversalSet()


try:
    from sqlalchemy.types import BigInteger
except ImportError:
    from sqlalchemy.databases.mysql import MSBigInteger as BigInteger
_type_map = {
    'bigint': BigInteger,
    'integer': types.Integer,
    'boolean': types.Boolean,
    'double': types.Float,
    'varchar': types.String,
    'timestamp': types.TIMESTAMP,
    'date': types.DATE,
}


class SupersqlCompiler(SQLCompiler):
    def visit_char_length_func(self, fn, **kw):
        return 'length{}'.format(self.function_argspec(fn, **kw))


class SupersqlDialect(default.DefaultDialect):
    name = 'supersql'
    driver = 'jpype'
    preparer = SupersqlIdentifierPreparer
    statement_compiler = SupersqlCompiler
    supports_alter = False
    supports_pk_autoincrement = False
    supports_default_values = False
    supports_empty_insert = False
    supports_unicode_statements = True
    supports_unicode_binds = True
    returns_unicode_strings = True
    description_encoding = None
    supports_native_boolean = True

    @classmethod
    def dbapi(cls):
        return supersql

    def create_connect_args(self, url):
        db_parts = (url.database or 'default').split('/')
        kwargs = {
            'host': url.host,
            'port': url.port or 7911,
        }
        kwargs.update(url.query)
        # if len(db_parts) == 1:
        # if 0==1:
        #     kwargs['catalog'] = db_parts[0]
        # elif len(db_parts) == 2:
        #     kwargs['catalog'] = db_parts[0]
        #     kwargs['schema'] = db_parts[1]
        # else:
        #     raise ValueError("Unexpected database format {}".format(url.database))
        return ([], kwargs)

    def get_schema_names(self, connection, **kw):
        return [row.Schema for row in connection.execute('SHOW SCHEMAS')]

    def _get_table_columns(self, connection, table_name, schema):
        full_table = self.identifier_preparer.quote_identifier(table_name)
        if schema:
            full_table = self.identifier_preparer.quote_identifier(schema) + '.' + full_table
        try:
            # rows = connection.execute('DESCRIBE {}'.format(full_table)).fetchall()
            return connection.execute('DESCRIBE {}'.format(full_table)).fetchall()
        except (supersql.DatabaseError, exc.DatabaseError) as e:
            # Normally SQLAlchemy should wrap this exception in sqlalchemy.exc.DatabaseError, which
            # it successfully does in the Hive version. The difference with Presto is that this
            # error is raised when fetching the cursor's description rather than the initial execute
            # call. SQLAlchemy doesn't handle this. Thus, we catch the unwrapped
            # presto.DatabaseError here.
            # Does the table exist?
            msg = (
                e.args[0].get('message') if e.args and isinstance(e.args[0], dict)
                else e.args[0] if e.args and isinstance(e.args[0], str)
                else None
            )
            regex = r"Table\ \'.*{}\'\ does\ not\ exist".format(re.escape(table_name))
            if msg and re.search(regex, msg):
                raise exc.NoSuchTableError(table_name)
            else:
                raise

    def has_table(self, connection, table_name, schema=None):
        try:
            self._get_table_columns(connection, table_name, schema)
            return True
        except exc.NoSuchTableError:
            return False

    def get_columns(self, connection, table_name, schema=None, **kw):
        rows = self._get_table_columns(connection, table_name, schema)

        # presto impl
        # result = []
        # for row in rows:
        #     try:
        #         coltype = _type_map[row.Type]
        #     except KeyError:
        #         util.warn("Did not recognize type '%s' of column '%s'" % (row.Type, row.Column))
        #         coltype = types.NullType
        #     result.append({
        #         'name': row.Column,
        #         'type': coltype,
        #         # newer Presto no longer includes this column
        #         'nullable': getattr(row, 'Null', True),
        #         'default': None,
        #     })
        # return result

        # hive impl
        rows = [[col.strip() if col else None for col in row] for row in rows]
        rows = [row for row in rows if row[0] and row[0] != '# col_name']
        result = []
        for (col_name, col_type, _comment) in rows:
            if col_name == '# Partition Information':
                break
            # Take out the more detailed type information
            # e.g. 'map<int,int>' -> 'map'
            #      'decimal(10,1)' -> decimal
            col_type = re.search(r'^\w+', col_type).group(0)
            if col_type.__eq__('int'):
                col_type = 'integer'
            elif col_type.__eq__('string'):
                col_type = 'varchar'
            try:
                coltype = _type_map[col_type]
            except KeyError:
                util.warn("Did not recognize type '%s' of column '%s'" % (
                    col_type, col_name))
                coltype = types.NullType
            result.append({
                'name': col_name,
                'type': coltype,
                'nullable': True,
                'default': None,
            })
        return result

    def get_foreign_keys(self, connection, table_name, schema=None, **kw):
        # Hive has no support for foreign keys.
        return []

    def get_pk_constraint(self, connection, table_name, schema=None, **kw):
        # Hive has no support for primary keys.
        return []

    def get_indexes(self, connection, table_name, schema=None, **kw):
        return []
        # rows = self._get_table_columns(connection, table_name, schema)
        # col_names = []
        # for row in rows:
        #     part_key = 'Partition Key'
        #     # Newer Presto moved this information from a column to the comment
        #     if (part_key in row and row[part_key]) or row['Comment'].startswith(part_key):
        #         col_names.append(row['Column'])
        # if col_names:
        #     return [{'name': 'partition', 'column_names': col_names, 'unique': False}]
        # else:
        #     return []

    def get_table_names(self, connection, schema=None, **kw):
        query = 'SHOW TABLES'
        if schema:
            query += ' FROM ' + self.identifier_preparer.quote_identifier(schema)
        return [row.Table for row in connection.execute(query)]

    def do_rollback(self, dbapi_connection):
        # No transactions for Presto
        pass

    def _check_unicode_returns(self, connection, additional_tests=None):
        # requests gives back Unicode strings
        return True

    def _check_unicode_description(self, connection):
        # requests gives back Unicode strings
        return True

if StrictVersion(sqlalchemy.__version__) < StrictVersion('0.7.0'):
    from pyhive import sqlalchemy_backports

    def reflecttable(self, connection, table, include_columns=None, exclude_columns=None):
        insp = sqlalchemy_backports.Inspector.from_engine(connection)
        return insp.reflecttable(table, include_columns, exclude_columns)
    SupersqlDialect.reflecttable = reflecttable
