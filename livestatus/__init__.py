import datetime
import time
import json
import logging
import re
import socket
import sqlite3
from collections import namedtuple
from multiprocessing import Process, Queue, Pipe


class LivestatusClient(object):
    '''A client class for Livestatus. This class is reusable for running
    multiple different queries against a set of monitor nodes. Querying
    against multiple monitors can be done in series or parallel.
    '''

    def __init__(self, monitors=[], parallel=False, workers=0):
        '''Constructor for the client

        Kwargs:
            monitors (many): This can be a dict, a MonitorNode object,
                or an iterable yielding more than one of either
            parallel (bool): If True, the client will retrieve data
                from monitor nodes in parallel
            workers (int): This sets the max number of worker threads
                to be launched with parallel. The deafult value of 0
                means that the number of threads launched will equal
                the number of monitor nodes
        '''
        self.monitors = []
        self.parallel = parallel
        self.workers  = workers if self.parallel else 1
        self.add_monitors(monitors)

    def add_monitors(self, monitors):
        '''A helper function for setting self.monitors'''
        if isinstance(monitors, MonitorNode):
            if all([
                    monitors.ip in [m.ip for m in self.monitors],
                    monitors.port in [m.port for m in self.monitors],
                    ]) \
                    or monitors.name in [m.name for m in self.monitors]:
                raise ValueError('Duplicate monitor')
            else:
                self.monitors.append(monitors)
        elif isinstance(monitors, dict):
            mon = MonitorNode(**monitors)
            self.add_monitors(mon)
        else:
            for monitor in monitors:
                self.add_monitors(monitor)

    def run(self, query):
        '''The main attraction for the livestatus client. This will run
        the provided query against all of the client's registered
        monitor nodes and provide a result set.

        Args:
            query (Query): A Query object

        Returns:
            QueryResultSet

        '''
        if query.auto_detect_types:
            col_types = self._get_column_datatypes(query)
            results = QueryResultSet(query, col_types=col_types)
        else:
            results = QueryResultSet(query)
        mon_queue = Queue()
        processes = []
        output = []

        for mon in self.monitors:
            mon_queue.put((mon, query))

        if self.workers == 0 or self.workers > len(self.monitors):
            self.workers = len(self.monitors)

        for w in xrange(0,self.workers):
            parent_conn, child_conn = Pipe()
            p = Process(target=monitor_worker, args=(mon_queue, child_conn))
            p.start()
            processes.append((p, parent_conn))
            mon_queue.put('STOP')

        live_procs = self.workers
        while live_procs > 0:
            for proc, conn in processes:
                if not conn.closed:
                    data = conn.recv()
                    if data == 'STOP':
                        live_procs -= 1
                        conn.close()
                    else:
                        results.update(*data)

        for p, conn in processes:
            p.join()

        return results

    def exec_sql(self, query, stmt):
        db = self.run(query).to_sqlite()
        return db.execute(stmt)

    def _get_column_datatypes(self, query):
        filters = []
        filters.append('table = {}'.format(query.table))
        col_filters = ['name = {}'.format(name) for name in query.columns]
        filters += col_filters
        or_filter = 'Or: {}'.format(len(col_filters))
        filters.append(or_filter)
        dt_query = Query('columns', ['name', 'type'],
                         ls_filters=filters,
                         )
        dt_results = self.run(dt_query)
        results = {}
        for column in query.columns:
            for row in dt_results.named_tuples:
                if row.name == column:
                    results[column] = row.type
                    break
        return results

    def __repr__(self):
        return '<LivestatusClient parallel: {}>'.format(self.parallel)


class MonitorNode(object):
    '''A simple class for holding monitor node info and running a
    query against that monitor
    '''

    def __init__(self, ip, port, name=None):
        self.ip   = ip
        self.port = port
        self.name = name if name is not None else ip

    def run_query(self, query):
        '''Perform a query against the monitor node

        Args:
            query (str): This should be a GET request for livestatus,
                NOT a Query object
        Returns:
            result (str)
        '''
        try:
            s = socket.create_connection((self.ip, self.port), 3)
            s.send(query)
            s.shutdown(socket.SHUT_WR)
            headers = s.recv(16)
        except socket.error:
            msg = 'Could not connect to {}:{}'.format(self.ip, self.port)
            raise MonitorNodeError(msg)
        try:
            if len(headers) != 16 or re.match('\d\d\d\s*\d+\s*\n', headers) is None:
                raise ValueError
            status = int(headers.split()[0])
            length = int(headers.split()[1])
        except (IndexError, ValueError):
            msg = '{} did not return a proper response header'.format(self.name)
            raise MonitorNodeError(msg)

        try:
            data = ''
            bytes_remaining = length
            BUFFER = 4096
            while bytes_remaining > 0:
                if bytes_remaining < BUFFER:
                    received = s.recv(bytes_remaining)
                else:
                    received = s.recv(BUFFER)
                bytes_remaining -= len(received)
                data += received
            s.close()
        except socket.error:
            msg = 'Lost connection with {} while receiving data'.format(self.name)
            raise MonitorNodeError(msg)
        return data, status, length

    def __repr__(self):
        return '<MonitorNode {}>'.format(self.ip)


class Query(object):
    '''Helper class for defining a query for livestatus, which also
    provides a means for filtering data after retrieval'''

    def __init__(self, table, columns=[], ls_filters=[], post_filters=[],
                 stats=[], omit_monitor_column=False, auto_detect_types=False):
        '''Query constructor

        Args:
            table (str): a Livestatus table name
            columns (list): which columns to select from the table
        Kwargs:
            ls_filters (list): any filters to be added to the query
                text
            post_filters (list): a list of callables that will be
                called on all column data
            stats (list): a list of statistics filters
            omit_monitor_column (bool): if true, results will not
                include monitor names in them, just the data from
                livestatus
            auto_detect_types (bool): if True, the LivestatusClient
                will first make a request to the 'columns' table in
                order to try to determine the datatype of each column.
                A QueryResultSet object will then convert column data
                appropriately.
        '''
        self.table               = table
        self.columns             = columns
        self.ls_filters          = ls_filters
        self.post_filters        = post_filters
        self.stats               = stats
        self.omit_monitor_column = omit_monitor_column
        self.auto_detect_types   = auto_detect_types
        if len(stats) > 0 and len(columns) > 1:
            msg = 'You cannot use more than one column with a stats query'
            raise ValueError(msg)

    @property
    def query_text(self):
        return Query.build(self.table,
                           self.columns,
                           self.ls_filters,
                           self.stats
                           )

    @staticmethod
    def build(table, columns, filters=[], stats=[]):
        '''
        Builds and returns a GET request string for the livestatus API
        Args:
            request (str): the table you want to pull from (e.g.
                services, hosts)
            columns (list): columns to select
            filters (list): any custom filters to add to the request
        '''

        query = 'GET {req}\n'.format(req=table)
        if columns:
            query += 'Columns: {cols}\n'.format(
                cols=' '.join(c for c in columns)
                )
        for f in filters:
            if any([
                    f.startswith('Or:'),
                    f.startswith('And:'),
                    f.startswith('Negate:'),
                    ]):
                query += f + '\n'
            else:
                query += 'Filter: {filter}\n'.format(filter=f)
        for s in stats:
            query += 'Stats: {s}\n'.format(s=s)
        query += 'ResponseHeader: fixed16\n'
        return query

    def __repr__(self):
        return '<Query for {} table>'.format(self.table)


class QueryResultSet(object):
    '''A class for storing and providing formatted results of data
    retrieved from livestatus
    '''

    def __init__(self, query, col_types={}, time_format='datetime'):
        '''QueryResultSet constructor

        Args:
            query (Query): a Query object
        Kwargs:
            col_types (dict): a dict with a mapping of types for each
                column represented in the result set.
            time_format (str): ('datetime' or 'stamp' accepted). If
                'datetime', timestamps will be converted to a datetime
                object. Otherwise they'll be left as a Unix timestamp
                (a float)
        '''
        self.query   = query
        self.results = {}
        self.col_types = col_types
        self.time_format = time_format
        self.columns = self.query.columns

    def update(self, monitor, data, error):
        self.results[monitor] = {}
        self.results[monitor]['data'] = data
        self.results[monitor]['error'] = error

    def to_sqlite(self):
        '''Dumps the query results into an sqlite database and returns
        a connection object to that database
        '''
        sqlite_type_index = {
            'str': 'TEXT',
            'int': 'INTEGER',
            'float': 'REAL',
            'time': 'REAL' if self.time_format == 'stamp' else 'TEXT'
        }
        rows = self._parse_data(format='lists',flatten=True)
        columns = []
        if not self.query.omit_monitor_column:
            columns.append('monitor TEXT')
        for c in self.columns:
            ctype = sqlite_type_index.get(self.col_types.get(c),'TEXT')
            columns.append(c + ' ' + ctype)
        create_kwargs = {
            'table': self.query.table,
            'columns': ', '.join(columns)
        }
        conn = sqlite3.connect(':memory:')
        create_query = 'CREATE TABLE {table}({columns})'.format(**create_kwargs)
        conn.execute(create_query)
        conn.commit()

        insert_q = 'INSERT INTO {table} VALUES ({params})'.format(
                table=self.query.table,
                params=','.join([
                                 '?' for item in
                                 create_kwargs['columns'].split(',')])
                )
        conn.executemany(insert_q, rows)
        error_create = 'CREATE TABLE errors (monitor TEXT, message TXT)'
        conn.execute(error_create)
        for monitor, error in self.errors.items():
            conn.execute('INSERT INTO errors VALUES (?,?)', (monitor, error))
        conn.commit()
        return conn

    @staticmethod
    def from_sql(sql):
        pass

    @property
    def json(self):
        '''Serialize results as a json string'''
        return json.dumps(self.dicts)

    @property
    def lists(self):
        '''Return a list of lists'''
        return self._parse_data(format='lists')

    @property
    def named_tuples(self):
        '''Return a list of namedtuple objects'''
        parsed = self.dicts
        if not self.query.omit_monitor_column:
            fields = ['monitor'] + self.columns
        else:
            fields = self.columns
        nt_row = namedtuple('Row', fields)
        return [nt_row(**row) for row in parsed]

    @property
    def dicts(self):
        '''Return a list of dict objects'''
        return self._parse_data()

    @property
    def errors(self):
        '''Returns a dict of monitor names and errors'''
        errors = {}
        for monitor in self.results.keys():
            if self.results[monitor]['error'] is not None:
                errors[monitor] = self.results[monitor]['error']
        return errors

    def _parse_data(self, format='dicts', flatten=False):
        '''Private method that parses raw data and returns it in a
        desired format

        Kwargs:
            format (str): 'dicts' for a list of dict objects, or
                'lists' for a list of list objects
        '''
        results = []
        for monitor in self.results.keys():
            data = self.results[monitor]['data']
            if data is None:
                continue
            data = data.strip('\n ')
            rows = data.split('\n')
            if self.query.columns == []:
                if len(self.query.stats) > 0:
                    cols = []
                    if len(self.query.columns) > 0:
                        cols.append(self.query.columns[0])
                    cols.extend(self.query.stats)
                    self.columns = cols
                else:
                    self.columns = rows[0].split(';')
                    del(rows[0])
            else:
                self.columns = self.query.columns
            for row in rows:
                if format == 'dicts':
                    row_dict = dict(zip(
                                        self.columns,
                                        self._apply_filters(row.split(';'))
                                        ))
                    row_dict = self._conv_types(row_dict,flatten=flatten)
                    if not self.query.omit_monitor_column:
                        row_dict['monitor'] = monitor
                    results.append(row_dict)
                elif format == 'lists':
                    row_list = []
                    if not self.query.omit_monitor_column:
                        row_list += [monitor]
                    items = self._conv_types(row.split(';'),flatten=flatten)
                    items = self._apply_filters(items)
                    row_list += items
                    results.append(row_list)
        return results

    def _conv_types(self, row, flatten=False):
        '''Private method used for converting return values from
        livestatus to their desired python data type
        '''
        converters = {
            'string': str,
            'float' : float,
            'int'   : int,
            'list'  : lambda x: x.split(','),
            'time'  : lambda x: datetime.datetime.fromtimestamp(float(x)),
            }
        if flatten:
            converters['list'] = str
            converters['time'] = lambda x: datetime.datetime.fromtimestamp(float(x)).isoformat(' ')

        if self.time_format == 'stamp':
            converters['time'] = float

        if isinstance(row, dict):
            for key, value in row.items():
                conv = converters.get(self.col_types.get(key), str)
                row[key] = conv(value)
            return row
        elif isinstance(row, list):
            result = []
            for key, value in zip(self.columns,row):
                conv = converters.get(self.col_types.get(key), str)
                result.append(conv(value))
            return result

    def _apply_filters(self, result_list):
        '''Private method that applies filters in
        QueryResultSet.query.post_filters to all column data
        '''
        for f in self.query.post_filters:
            result_list = map(f, result_list)
        return result_list

    def __len__(self):
        return len(self.lists)

    def __add__(self, other):
        if self.query.query_text != other.query.query_text:
            raise TypeError('QueryResultSet queries do not match')
        for monitor in other.results.keys():
            self.results[monitor] = {}
            self.results[monitor]['data'] = other.results[monitor]['data']
            self.results[monitor]['error'] = other.results[monitor]['error']
        return self


def monitor_worker(mon_queue, conn):
    '''
    Helper function for retrieving results from multiple monitors in parallel
    '''
    for monitor, query in iter(mon_queue.get, 'STOP'):
        data = None
        error = None
        try:
            data, status, length = monitor.run_query(query.query_text)
            if data is None or data.strip('\n\t ') == '':
                error = '{} did not return any data'.format(monitor.name)
                data = None
            elif status != 200:
                error = 'Error {code}: "{msg}"'.format(code=status, msg=data)
                data = None
        except Exception as e:
            error = e.message
        finally:
            conn.send((monitor.name, data, error))
    conn.send('STOP')
    return


class MonitorNodeError(Exception):
    pass
