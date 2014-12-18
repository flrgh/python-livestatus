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
