from . import *
from livestatus import *
import unittest


class TestMonitorNodeConstruction(unittest.TestCase):

    def test_init(self):
        mn = MonitorNode('1.2.3.4', 9999)

        self.assertEqual(mn.ip, '1.2.3.4')
        self.assertEqual(mn.name, '1.2.3.4')
        self.assertEqual(mn.port, 9999)


class TestMonitorQuery(unittest.TestCase):

    def setUp(self):
        self.server = ServerHelper(WellBehavedServer)
        self.host, self.port = self.server.start()
        self.monitor = MonitorNode(self.host, self.port)

    def test_good_query(self):
        query_text = 'GET services\n' + \
                     'Columns: col1 col2 col3 col4\n' + \
                     'Filter: state != 0\n' + \
                     'ResponseHeader: fixed16\n'

        data, status, length = self.monitor.run_query(query_text)

        # Did we get all the right types back?
        self.assertIsInstance(data, str)
        self.assertIsInstance(status, int)
        self.assertIsInstance(length, int)

        # Ded we send the right data?
        recvd = self.server.get_last_recv()
        self.assertEqual(query_text, recvd)

        # Did we get the right data back?
        self.assertEqual(data, 'string1;1;1418675988;1,2,3\nstring2;2;1418675987;a,b,c\n')
        self.assertEqual(status, 200)
        self.assertEqual(length, len(data))

    def test_query_without_headers(self):
        query_text = 'GET services\n' + \
                     'Columns: col1 col2 col3 col4\n' + \
                     'Filter: state != 0\n'

        self.assertRaises(MonitorNodeError, self.monitor.run_query,
                          query_text)

    def tearDown(self):
        self.server.stop()


class TestEmptyResponse(unittest.TestCase):

    def setUp(self):
        self.server = ServerHelper(EmptyResponseServer)
        self.host, self.port = self.server.start()
        self.monitor = MonitorNode(self.host, self.port)
        self.query_text = 'GET services\n' + \
                          'Columns: col1 col2 col3 col4\n' + \
                          'Filter: state != 0\n' + \
                          'ResponseHeader: fixed16\n'

    def test_empty_response(self):
        data, status, length = self.monitor.run_query(self.query_text)
       
        self.assertEqual(data, '')
        self.assertEqual(status, 200)
        self.assertEqual(length, 0)

    def tearDown(self):
        self.server.stop()


class TestNoData(unittest.TestCase):

    def setUp(self):
        self.server = ServerHelper(NoDataServer)
        self.host, self.port = self.server.start()
        self.monitor = MonitorNode(self.host, self.port)
        self.query_text = 'GET services\n' + \
                          'Columns: col1 col2 col3 col4\n' + \
                          'Filter: state != 0\n' + \
                          'ResponseHeader: fixed16\n'

    def test_no_data(self):

        self.assertRaises(MonitorNodeError, self.monitor.run_query,
                          self.query_text)
        
    def tearDown(self):
        self.server.stop()


class TestTimeout(unittest.TestCase):

    def setUp(self):
        self.server = ServerHelper(TimeoutServer)
        self.host, self.port = self.server.start()
        self.monitor = MonitorNode(self.host, self.port)

        self.query_text = 'GET services\n' + \
                          'Columns: col1 col2 col3 col4\n' + \
                          'Filter: state != 0\n' + \
                          'ResponseHeader: fixed16\n'

    def test_monitor_timeout(self):
        self.assertRaises(MonitorNodeError, self.monitor.run_query,
                          self.query_text)
    def tearDown(self):
        self.server.stop()
    

class TestDeadServer(unittest.TestCase):

    def setUp(self):
        self.server = ServerHelper(DeadServer)
        self.host, self.port = self.server.start()
        self.monitor = MonitorNode(self.host, self.port)

        self.query_text = 'GET services\n' + \
                          'Columns: col1 col2 col3 col4\n' + \
                          'Filter: state != 0\n' + \
                          'ResponseHeader: fixed16\n'
    def test_no_connection(self):
        self.assertRaises(MonitorNodeError, self.monitor.run_query,
                          self.query_text)

    def tearDown(self):
        self.server.stop()


class TestRudeServer(unittest.TestCase):

    def setUp(self):
        self.server = ServerHelper(RudeServer)
        self.host, self.port = self.server.start()
        self.monitor = MonitorNode(self.host, self.port)

        self.query_text = 'GET services\n' + \
                          'Columns: col1 col2 col3 col4\n' + \
                          'Filter: state != 0\n' + \
                          'ResponseHeader: fixed16\n'

    def test_connection_hung_up(self):
        self.assertRaises(MonitorNodeError, self.monitor.run_query,
                          self.query_text)
    def tearDown(self):
        self.server.stop()
