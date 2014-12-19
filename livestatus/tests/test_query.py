import unittest
from livestatus import Query


class TestQueryConstruction(unittest.TestCase):

    def test_init(self):
        q = Query('table')

        self.assertEqual(q.table, 'table')
        self.assertEqual(q.columns, [])
        self.assertEqual(q.ls_filters, [])
        self.assertEqual(q.post_filters, [])
        self.assertFalse(q.omit_monitor_column)
        self.assertFalse(q.auto_detect_types)


class TestQueryBuilder(unittest.TestCase):

    def test_build(self):

        # No kwargs
        q = Query('table')
        expected = 'GET table\n' + \
                   'ResponseHeader: fixed16\n'
        self.assertEqual(q.query_text, expected)

        # Columns
        q = Query('table', ['col1','col2'])
        expected = 'GET table\n' + \
                   'Columns: col1 col2\n' + \
                   'ResponseHeader: fixed16\n'
        self.assertEqual(q.query_text, expected)

        # Columns and filters:
        q = Query('table', ['col1', 'col2'], ['1 = 2'])
        expected = 'GET table\n' + \
                   'Columns: col1 col2\n' + \
                   'Filter: 1 = 2\n' + \
                   'ResponseHeader: fixed16\n'
        self.assertEqual(q.query_text, expected)

    def test_build_special_filters(self):
        q = Query('table', ['col1', 'col2'],
                  ['1 = 2', '3 = 4', 'Or: 2'])

        expected = 'GET table\n' + \
                   'Columns: col1 col2\n' + \
                   'Filter: 1 = 2\n' + \
                   'Filter: 3 = 4\n' + \
                   'Or: 2\n' + \
                   'ResponseHeader: fixed16\n'

        self.assertEqual(expected, q.query_text)

    def test_build_stats(self):
        q = Query('table', stats=['state != 0'])
        expected = 'GET table\nStats: state != 0\nResponseHeader: fixed16\n'
        self.assertEqual(expected, q.query_text)

        self.assertRaises(ValueError, Query.__init__, q, 'table',
                          columns=['col1','col2'],
                          stats=['state = 0', 'state = 1'])
