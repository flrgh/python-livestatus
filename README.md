python-livestatus
=================

A Python library for the Livestatus API

### Example usage:
```
>>> from livestatus import LivestatusClient, Query, MonitorNode
>>> 
>>> lc = LivestatusClient(parallel=False)
>>> 
>>> monitor = MonitorNode('1.1.1.1', port=9999, name='my-monitor01')
>>> lc.add_monitors(monitor)
>>> 
>>> query = Query(table='some-table',
>>>               columns=['col1', 'col2','col3'],
>>>               filters=['col1 != 0']
>>>              )
>>> 
>>> result_set = lc.run(query)
>>> 
>>> print result_set.lists
[['my-monitor01', '1', 'val1', 'val2'], ['my-monitor01', '1', 'foo', 'bar']]
>>> print result_set.dicts
[
    {
        'monitor': 'my-monitor01',
        'col1'   : '1',
        'col2'   : 'val1',
        'col3'   : 'val2'
    },
    {
        'monitor': 'my-monitor01'
        'col1'   : '1',
        'col2'   : 'foo',
        'col3'   : 'bar'
    }
]
```
