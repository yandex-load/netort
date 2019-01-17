from netort.data_manager import DataSession
import logging

logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.DEBUG)


def get_uploader(data_session, column_mapping, hostname, source):
    """
    :param column_mapping: {column_name: (metric_name, group)}
    :type data_session: DataSession
    """
    _router = {}

    def get_router(tags):
        if set(tags) - set(_router.keys()):
            [_router.setdefault(tag,
                                {col_name:
                                     data_session.new_tank_metric(name,
                                                                  hostname,
                                                                  group,
                                                                  source,
                                                                  ammo_tag=tag)
                                 for col_name, (name, group) in column_mapping.items()})
             for tag in tags]
        return _router

    def upload_df(df):
        router = get_router(df.tag.unique().tolist())
        for tag, df_tag in df.groupby('tag'):
            for col_name, metric in router[tag].items():
                df_tag['value'] = df_tag[col_name]
                metric.put(df_tag)

    return upload_df

groups = {
    'fractions': ['connect_time', 'send_time',
                  'latency', 'receive_time',
                  'interval_event'],
    'size': ['size_out', 'size_in',]
}

col_map = {name: (name, group) for group, names in groups.items() for name in names}

clients = [{'type': 'luna', 'api_address': 'https://volta-back.yandex-team.ru/'}]