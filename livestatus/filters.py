### Filters to be run on livestatus data


def empty_to_nonetype(data):
    '''Return a NoneType object if data is an empty string'''
    data = data.strip('\n\t ')
    if data == '':
        return None
    else:
        return data


def detect_numbers(data):
    '''Convert data to an int or float if possible'''
    if data.isdigit():
        if '.' in data:
            return float(data)
        else:
            return int(data)
    else:
        return data
