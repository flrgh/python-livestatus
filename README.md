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
>>>               ls_filters=['col1 != 0']
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

The `monitor` column is added post-query as this is not provided by livestatus. You can disable this behavior by setting a query's `omit_monitor_column` attribute to `True`:

```
>>> result_set.query.omit_monitor_column = True
>>> print result_set.dicts
[
    {
        'col1'   : '1',
        'col2'   : 'val1',
        'col3'   : 'val2'
    },
    {
        'col1'   : '1',
        'col2'   : 'foo',
        'col3'   : 'bar'
    }
]
```

Basic callable filters can be registered on a Query object, which will be applied after querying data:


```
>>> print result_set.lists
[['my-monitor01', '1', 'val1', 'val2'], ['my-monitor01', '1', '', 'bar']]
>>> 
>>> result_set.query.post_filters = [empty_to_nonetype, detect_numbers]
>>>
>>> result_set.query.post_filters.append(lambda x: x.upper() if isinstance(x, str) else x)
>>> 
>>> print result_set.lists
[['MY-MONITOR01', 1, 'VAL1', 'VAL2'], ['MY-MONITOR01', 1, None, 'BAR']]
```
