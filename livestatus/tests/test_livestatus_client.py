import unittest
from livestatus import *


class TestLivestatusClientConstruction(unittest.TestCase):

    def setUp(self):
        self.single_monitor_dict = {
                'name': 'my-monitor',
                'ip' : '1.2.3.4',
                'port': 9999
                }
        self.single_monitor_node = MonitorNode(**self.single_monitor_dict)
        self.other_monitor_dict = {
                'name': 'my-monitor2',
                'ip': '4.3.2.1',
                'port': 9999
                }
        self.other_monitor_node = MonitorNode(**self.other_monitor_dict)

    def test_init_no_args(self):
        # No args/kwargs
        ls = LivestatusClient()
        self.assertIsInstance(ls, LivestatusClient)
        self.assertEqual(ls.monitors, [])
        self.assertFalse(ls.parallel)
        self.assertEqual(ls.workers, 1)
      
    def test_init_parallel_true(self):
        # parallel=True
        ls = LivestatusClient(parallel=True)
        self.assertTrue(ls.parallel)
        self.assertEqual(ls.workers, 0)

    def test_init_workers_no_parallel(self):
        # workers=5 with parallel left False
        ls = LivestatusClient(workers=5)
        self.assertFalse(ls.parallel)
        self.assertEqual(ls.workers, 1)

    def test_init_parallel_with_workers(self):
        # parallel=True, workers=5
        ls = LivestatusClient(parallel=True, workers=5)
        self.assertTrue(ls.parallel)
        self.assertEqual(ls.workers, 5)

    def test_init_with_monitors(self):
        # test instantiating w/ monitor dict
        ls = LivestatusClient(monitors=self.single_monitor_dict)
        self.assertEqual(len(ls.monitors), 1)
        self.assertIsInstance(ls.monitors[0], MonitorNode)

        # Instantiate with monitor dict in a list
        ls = LivestatusClient(monitors=[self.single_monitor_dict])
        self.assertEqual(len(ls.monitors), 1)
        self.assertIsInstance(ls.monitors[0], MonitorNode)

        # test instantiating w/ MonitorNode object
        ls = LivestatusClient(monitors=self.single_monitor_node)
        self.assertEqual(len(ls.monitors), 1)
        self.assertIsInstance(ls.monitors[0], MonitorNode)

        # Test with mixed types

        ls = LivestatusClient(monitors=[self.single_monitor_dict,
                                        self.other_monitor_node])
        self.assertEqual(len(ls.monitors), 2)
        self.assertIsInstance(ls.monitors[0], MonitorNode)
        self.assertIsInstance(ls.monitors[1], MonitorNode)


class TestLivestatusAddMonitors(unittest.TestCase):

    def setUp(self):
        self.monitor1_as_dict = {
                'name': 'my-monitor01',
                'ip'  : '1.2.3.4',
                'port': 4000
                }
        self.monitor1_as_object = MonitorNode(**self.monitor1_as_dict)

        self.monitor2_as_dict = {
                'name': 'my-monitor01',
                'ip'  : '1.2.3.5',
                'port': 4001
                }
        self.monitor2_as_object = MonitorNode(**self.monitor2_as_dict)

    def test_add_single_monitor(self):

        # ...as a dict
        ls = LivestatusClient()
        ls.add_monitors(self.monitor1_as_dict)
        self.assertEqual(len(ls.monitors), 1)
        self.assertIsInstance(ls.monitors[0], MonitorNode)

        # ...as an object
        ls = LivestatusClient()
        ls.add_monitors(self.monitor1_as_object)
        self.assertEqual(len(ls.monitors), 1)
        self.assertIsInstance(ls.monitors[0], MonitorNode)

        # ...as a singleton
        ls = LivestatusClient()
        ls.add_monitors([self.monitor1_as_object])
        self.assertEqual(len(ls.monitors), 1)
        self.assertIsInstance(ls.monitors[0], MonitorNode)

        ls = LivestatusClient()
        ls.add_monitors([self.monitor1_as_dict])
        self.assertEqual(len(ls.monitors), 1)
        self.assertIsInstance(ls.monitors[0], MonitorNode)
