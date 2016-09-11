import itertools
import pandas as pd

def _get_field(node, path):
    if not path:
        yield node
        return
    value = node.get(path[0], [])
    if isinstance(value, list):
        for v in value:
            for x in _get_field(v, path[1:]):
                yield x
    else:
        for x in _get_field(value, path[1:]):
            yield x

def json_query(data, *query_elems):
    """
    Queries a set of JSON records for specific fields and returns a pandas
    DataFrame with all records flattened. Dots are used to specify that field is
    nested within the record.

    Example usage:
      q = json_query(data, 'tenderID', 'status', 'bids.value.amount')
    """
    paths = [x.split('.') for x in query_elems]
    def generator():
        for elem in data:
            row = []
            for path in paths:
                row.append(_get_field(elem, path))
            for x in itertools.product(*row):
                yield x
    return pd.DataFrame(generator(), columns=query_elems)
