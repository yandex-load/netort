import collections


def pretty_print(req):
    return '{header}\n{query}\n{http_headers}\n\n{body}\n{footer}'.format(
        header='-----------QUERY START-----------',
        query=req.method + ' ' + req.url,
        http_headers='\n'.join('{}: {}'.format(k, v) for k, v in req.headers.items()),
        body=req.body,
        footer='-----------QUERY END-----------'
    )


def recursive_dict_update(d1, d2):
    for k, v in d2.items():
        if isinstance(v, collections.Mapping):
            r = recursive_dict_update(d1.get(k, {}), v)
            d1[k] = r
        else:
            d1[k] = d2[k]
    return d1