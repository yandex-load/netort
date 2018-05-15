def pretty_print(req):
    """
    At this point it is completely built and ready
    to be fired; it is "prepared".

    However pay attention at the formatting used in
    this function because it is programmed to be pretty
    printed and may differ from the actual request.
    """
    return '{header}\n{query}\n{http_headers}\n\n{body}\n{footer}'.format(
        header='-----------QUERY START-----------',
        query=req.method + ' ' + req.url,
        http_headers='\n'.join('{}: {}'.format(k, v) for k, v in req.headers.items()),
        body=req.body,
        footer='-----------QUERY END-----------'
    )
