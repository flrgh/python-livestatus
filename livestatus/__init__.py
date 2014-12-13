import json
import logging
import socket
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
            self.monitors.append(monitors)
        elif isinstance(monitors, dict):
            self.monitors.append(MonitorNode(**monitors))
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
            QueryResult

        '''
        results = QueryResult(query)
        mon_queue = Queue()
        processes = []
        output = []

        for mon in self.monitors:
            mon_queue.put((mon, query))

        if self.workers == 0:
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

    def __repr__(self):
        return '<LivestatusClient parallel: {}>'.format(self.parallel)


class MonitorNode(object):
    '''A simple class for holding monitor node info and running a
    query against that monitor
    '''

    def __init__(self, ip, port=9999, name=None):
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
        s = socket.create_connection((self.ip, self.port))
        s.send(query)
        s.shutdown(socket.SHUT_WR)
        result = ''
        while True:
            received = s.recv(4096)
            if received == '':
                break
            else:
                result += received
        s.close()
        return result

    def __repr__(self):
        return '<MonitorNode {}>'.format(self.ip)


class Query(object):
    '''Helper class for defining a query for livestatus, which also
    provides a means for filtering data after retrieval'''

    def __init__(self, table, columns, ls_filters=[], post_filters=[],
                 omit_monitor_column=False):
        '''Query constructor

        Args:
            table (str): a Livestatus table name
            columns (list): which columns to select from the table
        Kwargs:
            ls_filters (list): any filters to be added to the query
                text
            post_filters (list): a list of callables that will be
                called on all column data
            omit_monitor_column (bool): if true, results will not
                include monitor names in them, just the data from
                livestatus
        '''
        self.table               = table
        self.columns             = columns
        self.ls_filters          = ls_filters
        self.post_filters        = post_filters
        self.omit_monitor_column = omit_monitor_column


    @property
    def query_text(self):
        return Query.build(self.table,
                           self.columns,
                           self.ls_filters
                           )

    @staticmethod
    def build(table, columns, filters=[]):
        '''
        Builds and returns a GET request string for the livestatus API
        Args:
            request (str): the table you want to pull from (e.g.
                services, hosts)
            columns (list): columns to select
            filters (list): any custom filters to add to the request
        '''

        query = 'GET {req}\n'.format(req=table)
        query += 'Columns: {cols}\n'.format(
            cols=' '.join(c for c in columns)
            )
        for f in filters:
            query += 'Filter: {filter}\n'.format(filter=f)
        return query

    def __repr__(self):
        return '<Query for {} table>'.format(self.table)


class QueryResult(object):
    '''A class for storing and providing formatted results of data
    retrieved from livestatus
    '''

    def __init__(self, query, monitor=None, data=None, error=None):
        '''QueryResult constructor

        Args:
            query (Query): a Query object
        Kwargs:
            monitor (str): a monitor name/alias
            data (str): the raw data from a livestatus query
            error (str or Exception): any errors/warnings raised when
                querying the monitor
        '''
        self.query   = query
        self.results = {}
        if monitor is not None and (data is not None or error is not None):
            self.update(monitor, data, error)

    def update(self, monitor, data, error):
        self.results[monitor] = {}
        self.results[monitor]['data'] = data
        self.results[monitor]['error'] = error

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
        if not self.query.omit_monitor_column:
            fields = ['monitor'] + self.query.columns
        else:
            fields = self.query.columns
        nt_row = namedtuple('Row', fields)
        return [nt_row(**row) for row in self.dicts]

    @property
    def dicts(self):
        '''Return a list of dict objects'''
        return self._parse_data(format='dicts')

    @property
    def errors(self):
        '''Returns a dict of monitor names and errors'''
        errors = {}
        for monitor in self.results.keys():
            if self.results[monitor]['error'] is not None:
                errors[monitor] = self.results[monitor]['error']
        return errors

    def _parse_data(self, format='dicts'):
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
            for row in rows:
                if format == 'dicts':
                    row_dict = dict(zip(
                                        self.query.columns,
                                        self._apply_filters(row.split(';'))
                                        ))
                    if not self.query.omit_monitor_column:
                        row_dict['monitor'] = monitor
                    results.append(row_dict)
                elif format == 'lists':
                    row_list = []
                    if not self.query.omit_monitor_column:
                        row_list += [monitor]
                    row_list += self._apply_filters(row.split(';'))
                    results.append(row_list)
        return results


    def _apply_filters(self, result_list):
        '''Private method that applies filters in
        QueryResult.query.post_filters to all column data
        '''
        for f in self.query.post_filters:
            result_list = map(f, result_list)
        return result_list

    def __len__(self):
        return len(self.lists)

    def __add__(self, other):
        if self.query.query_text != other.query.query_text:
            raise TypeError('QueryResult queries do not match')
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
            data = monitor.run_query(query.query_text)
            if data.strip('\n ') == '':
                data = None
                error = '{} did not return any data'.format(monitor.name)
        except Exception as e:
            error = e
        finally:
            conn.send((monitor.name, data, error))
    conn.send('STOP')
    return
