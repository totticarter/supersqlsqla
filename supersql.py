"""DB-API implementation backed by Presto

See http://www.python.org/dev/peps/pep-0249/

Many docstrings in this file are based on the PEP, which is in the public domain.
"""

from __future__ import absolute_import
from __future__ import unicode_literals

from TCLIService import ttypes
from builtins import object
from PySupersql import common
from PySupersql.common import DBAPITypeObject
# Make all exceptions visible in this module per DB-API
from pyhive.exc import *  # noqa
import base64
import getpass
import logging
import requests
import jpype

try:  # Python 3
    import urllib.parse as urlparse
except ImportError:  # Python 2
    import urlparse


# PEP 249 module globals
apilevel = '2.0'
threadsafety = 2  # Threads may share the module and connections.
paramstyle = 'pyformat'  # Python extended format codes, e.g. ...WHERE name=%(name)s

_logger = logging.getLogger(__name__)
_escaper = common.ParamEscaper()


def connect(*args, **kwargs):
    """Constructor for creating a connection to the database. See class :py:class:`Connection` for
    arguments.

    :returns: a :py:class:`Connection` object.
    """
    return Connection(*args, **kwargs)


class Connection(object):
    """Presto does not have a notion of a persistent connection.

    Thus, these objects are small stateless factories for cursors, which do all the real work.
    """

    def __init__(self, *args, **kwargs):
        self._args = args
        self._kwargs = kwargs

        import logging, os
        logging.basicConfig(filename=os.path.join('/Users/waixingren/PycharmProjects/sql', 'log.txt'), level=logging.DEBUG)
        #/Users/waixingren/PycharmProjects/sql
        logging.debug('begin to load class')

        supersqljdbcJars = ["/Users/waixingren/software/tencent/uaejdbc/supersql-jdbc/target/uaejdbc-1.0-SNAPSHOT-jar-with-dependencies.jar"]
        jvm_path = jpype.getDefaultJVMPath()
        jvm_cp = "-Djava.class.path={}".format(":".join(supersqljdbcJars))
        jpype.startJVM(jvm_path, jvm_cp)
        logging.debug('jar loaed and jvm started, begin to load class')

        javaClass = jpype.JClass('com.tencent.supersql.jdbc.SSqlDriver')
        logging.debug('ssqldriver loaded')
        str1 = 'supersql://' + self._kwargs.get('host') + ':' + str(self._kwargs.get('port'))
        hostport = str1[str1.index(':'): str1.__len__()]
        ssqljdbcurl = "jdbc:ssql" + hostport + "/default"
        self._connection = jpype.java.sql.DriverManager.getConnection(ssqljdbcurl, "", "")
        kwargs['connection']=self._connection


    def close(self):
        # self._connection.close
        pass

    def commit(self):
        """Presto does not support transactions"""
        pass

    def cursor(self):
        """Return a new :py:class:`Cursor` object using the connection."""
        return Cursor(*self._args, **self._kwargs)

    def rollback(self):
        raise NotSupportedError("Presto does not have transactions")  # pragma: no cover


class Cursor(common.DBAPICursor):
    """These objects represent a database cursor, which is used to manage the context of a fetch
    operation.

    Cursors are not isolated, i.e., any changes done to the database by a cursor are immediately
    visible by other cursors or connections.
    """

    def __init__(self, host, connection, port='7911', schema='default', poll_interval=1):
        """
        :param host: hostname to connect to the supersql thrift server e.g. ``supersql.example.com``
        :param port: int -- port, defaults to 7911
        """
        super(Cursor, self).__init__(poll_interval)
        # Config
        self._host = host
        self._port = port
        self._schema = schema
        self._arraysize = 1
        self._poll_interval = poll_interval
        self._reset_state()
        self._connection=connection

    def _reset_state(self):
        """Reset state about the previous query in preparation for running another query"""
        super(Cursor, self)._reset_state()
        self._nextUri = None
        self._columns = None

    @property
    def description(self):
        """This read-only attribute is a sequence of 7-item sequences.

        Each of these sequences contains information describing one result column:

        - name
        - type_code
        - display_size (None in current implementation)
        - internal_size (None in current implementation)
        - precision (None in current implementation)
        - scale (None in current implementation)
        - null_ok (always True in current implementation)

        The ``type_code`` can be interpreted by comparing it to the Type Objects specified in the
        section below.
        """
        # Sleep until we're done or we got the columns
        self._fetch_while(
            lambda: self._columns is None and
            self._state not in (self._STATE_NONE, self._STATE_FINISHED)
        )
        # if self._columns is None:
        #     return None
        # return [
        #     # name, type_code, display_size, internal_size, precision, scale, null_ok
        #     (col['name'], col['type'], None, None, None, None, True)
        #     for col in self._columns
        # ]

        self._description = []
        resultSetMetaData = self._columns.getMetaData()
        columnCount = resultSetMetaData.getColumnCount()
        from PySupersql.common import _VALUES_TO_NAMES
        for i in range(1,columnCount+1):
            _type = resultSetMetaData.getColumnType(i)
            type_code = _VALUES_TO_NAMES[_type]
            self._description.append((
                resultSetMetaData.getColumnName(i).decode('utf-8'), type_code.decode('utf-8'),None, None, None, None, True
            ))
        return self._description

    def execute(self, operation, parameters=None):


        # Prepare statement
        if parameters is None:
            sql = operation
        else:
            sql = operation % _escaper.escape_args(parameters)

        self._reset_state()
        self._state = self._STATE_RUNNING

        self._connection.setSchema('default')
        statement=self._connection.createStatement()
        if sql.startswith('SELECT'):
            sql = 'select count(*) as count_1 from kylin_sales'
        else:
            sql = 'desc nationhive'
        resultset = statement.executeQuery(sql)
        self._columns = resultset
        self._process_response()

    def poll(self):
        """Poll for and return the raw status data provided by the Presto REST API.

        :returns: dict -- JSON status information or ``None`` if the query is done
        :raises: ``ProgrammingError`` when no query has been started

        .. note::
            This is not a part of DB-API.
        """
        if self._state == self._STATE_NONE:
            raise ProgrammingError("No query yet")
        if self._nextUri is None:
            assert self._state == self._STATE_FINISHED, "Should be finished if nextUri is None"
            return None
        response = requests.get(self._nextUri)
        self._process_response(response)
        return response.json()

    def _fetch_more(self):
        """Fetch the next URI and update state"""
        # self._process_response(requests.get(self._nextUri))

        resultSet = self._columns
        # self._decode_binary(new_data)
        while resultSet.next():
            columnName = resultSet.getColumn
        oneRow = []
        oneRow.append()

    def _decode_binary(self, rows):
        # As of Presto 0.69, binary data is returned as the varbinary type in base64 format
        # This function decodes base64 data in place
        for i, col in enumerate(self.description):
            if col[1] == 'varbinary':
                for row in rows:
                    row[i] = base64.b64decode(row[i])

    def _process_response(self):
        """Given the JSON response from Presto's REST API, update the internal state with the next
        URI and any data from the response
        """
        # TODO handle HTTP 503
        # if response.status_code != requests.codes.ok:
        #     fmt = "Unexpected status code {}\n{}"
        #     raise OperationalError(fmt.format(response.status_code, response.content))
        # response_json = response.json()
        # _logger.debug("Got response %s", response_json)
        # assert self._state == self._STATE_RUNNING, "Should be running if processing response"
        # self._nextUri = response_json.get('nextUri')
        # self._columns = response_json.get('columns')
        # if 'X-Presto-Clear-Session' in response.headers:
        #     propname = response.headers['X-Presto-Clear-Session']
        #     self._session_props.pop(propname, None)
        # if 'X-Presto-Set-Session' in response.headers:
        #     propname, propval = response.headers['X-Presto-Set-Session'].split('=', 1)
        #     self._session_props[propname] = propval
        # if 'data' in response_json:
        #     assert self._columns
        #     new_data = response_json['data']
        #     self._decode_binary(new_data)
        #     self._data += map(tuple, new_data)
        # if 'nextUri' not in response_json:
        #     self._state = self._STATE_FINISHED
        # if 'error' in response_json:
        #     assert not self._nextUri, "Should not have nextUri if failed"
        #     raise DatabaseError(response_json['error'])

        #process response for supersql
        resultSet = self._columns
        resultSetMetaData = resultSet.getMetaData()
        column_count = resultSetMetaData.getColumnCount()
        # self._data = []
        while resultSet.next():
            one_row = []
            for i in range(1, column_count+1):
                column_type = resultSetMetaData.getColumnType(i)
                if column_type == 4:
                    column_value = resultSet.getInt(i)
                    one_row.append(column_value)
                elif column_type == 12:
                    column_value = resultSet.getString(i)
                    one_row.append(column_value)
                elif column_type == -5:
                    column_value = resultSet.getLong(i)
                    one_row.append(column_value)
            self._data.append(one_row)

        self._state = self._STATE_FINISHED
#
# Type Objects and Constructors
#


# See types in presto-main/src/main/java/com/facebook/presto/tuple/TupleInfo.java
FIXED_INT_64 = DBAPITypeObject(['bigint'])
VARIABLE_BINARY = DBAPITypeObject(['varchar'])
DOUBLE = DBAPITypeObject(['double'])
BOOLEAN = DBAPITypeObject(['boolean'])
