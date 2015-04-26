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
>>>               )
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

Callable filters can be registered on a Query object, which will be applied after querying data:


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

You can optionally set your query's `auto_detect_types` attribute to `True` to handle data conversions. The client will query the livestatus `columns` table to retreive data types and then convert the data from the resulting livestatus query:

```
>>> query = Query(table='services',
>>>               columns=['host_name', 'state', 'host_last_check', 'host_services'],
>>>               ls_filters=['state != 0'],
>>>               auto_detect_types=True
>>>              )
>>> 
>>> result_set = lc.run(query)
>>> 
>>> print result_set.dicts
[
    {
        'monitor'        : 'my-monitor01',
        'host_name'      : 'my_host',
        'state'          : 1,
        'host_last_check': datetime.datetime(2014, 12, 14, 7, 32, 12),
        'host_services'  : ['smtp', 'ssh']
    },
    {
        'monitor'        : 'my-monitor01'
        'host_name'      : 'my_other_host,
        'state'          : 2,
        'host_last_check': datetime.datetime(2014, 12, 14, 7, 31, 5),
        'host_services'  : ['ssh', 'http']
    }
]
```
