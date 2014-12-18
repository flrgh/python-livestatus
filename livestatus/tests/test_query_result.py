import unittest
from collections import namedtuple
from livestatus import QueryResultSet, Query


class TestQueryResultMinArgs(unittest.TestCase):

    def setUp(self):
        self.q = Query('table')
        self.result_set = QueryResultSet(self.q)
        self.monitor1 = {
                'monitor' : 'my-monitor01',
                'data'    : 'col1;col2;col3\nn1;n2;n3\n',
                'error'   : None
                }
        self.monitor2 = {
                'monitor' : 'my-monitor02',
                'data'    : None,
                'error'   : 'my-monitor02 did not respond'
                }

    def test_constructor(self):

        self.assertIsInstance(self.result_set.query, Query)
        self.assertEqual(self.result_set.col_types, {})
        self.assertEqual(self.result_set.results, {})
        self.assertEqual(self.result_set.time_format, 'datetime')

    def test_update(self):

        self.result_set.update(**self.monitor1)
        expected = {'data': 'col1;col2;col3\nn1;n2;n3\n',
                    'error': None}
        self.assertEqual(self.result_set.results[self.monitor1['monitor']],
                         expected)

        self.result_set.update(**self.monitor2)

        expected = {'data':None,'error':'my-monitor02 did not respond'}
        self.assertEqual(self.result_set.results[self.monitor2['monitor']],
                         expected)

    def test_result_parsing(self):
        self.result_set.update(**self.monitor1)
        self.result_set.update(**self.monitor2)

        # As dicts
        expected = [{'monitor':'my-monitor01','col1':'n1','col2':'n2',
                     'col3':'n3'}]
        actual = self.result_set.dicts
        self.assertEqual(len(actual), len(expected))
        for key, val in expected[0].items():
            self.assertIn(key, actual[0].keys())
            self.assertEqual(actual[0][key], val)

        # As lists
        expected = [['my-monitor01', 'n1', 'n2', 'n3']]
        actual = self.result_set.lists
        self.assertEqual(expected, actual)

        # As named tuples
        self.assertEqual(len(self.result_set.named_tuples),1)
        nt = self.result_set.named_tuples[0]
        self.assertEqual(nt.monitor, 'my-monitor01')
        self.assertEqual(nt.col1, 'n1')
        self.assertEqual(nt.col2, 'n2')
        self.assertEqual(nt.col3, 'n3')
