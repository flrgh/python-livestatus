### Filters to be run on livestatus data


def empty_to_nonetype(data):
    data = data.strip('\n\t ')
    if data == '':
        return None
    else:
        return data


def detect_numbers(data):
    if data.isdigit():
        if '.' in data:
            return float(data)
        else:
            return int(data)
    else:
        return data
