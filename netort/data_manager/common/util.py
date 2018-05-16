def pretty_print(req):
    return '{header}\n{query}\n{http_headers}\n\n{body}\n{footer}'.format(
        header='-----------QUERY START-----------',
        query=req.method + ' ' + req.url,
        http_headers='\n'.join('{}: {}'.format(k, v) for k, v in req.headers.items()),
        body=req.body,
        footer='-----------QUERY END-----------'
    )
