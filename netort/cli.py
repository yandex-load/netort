import argparse

from datetime import datetime

import signal
from netort.data_manager import DataSession
from yandextank.plugins.Phantom.reader import string_to_df_microsec
import logging

logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.DEBUG)


def get_handler(data_session):
    def handler(signum, frame):
        data_session.interrupt()
    return handler


def get_uploader(data_session, column_mapping, overall_only=False):
    """
    :type data_session: DataSession
    """
    _router = {}
    _overall = {
        'interval_real': data_session.new_true_metric('interval_real overall', raw=False, aggregate=True),
        'connect_time': data_session.new_true_metric('connect_time overall', raw=False, aggregate=True),
        'send_time': data_session.new_true_metric('send_time overall', raw=False, aggregate=True),
        'latency': data_session.new_true_metric('latency overall', raw=False, aggregate=True),
        'receive_time': data_session.new_true_metric('receive_time overall', raw=False, aggregate=True),
        'interval_event': data_session.new_true_metric('interval_event overall', raw=False, aggregate=True),
        'net_code': data_session.new_event_metric('net_code overall', raw=False, aggregate=True),
        'proto_code': data_session.new_event_metric('proto_code overall', raw=False, aggregate=True)}

    def get_router(tags):
        """
        :param tags:
        :return: {'%tag': {'%column_name': metric_object(name, group)}}
        """
        if set(tags) - set(_router.keys()):
            [_router.setdefault(tag,
                                {col_name: data_session.new_true_metric(name + '-' + tag,
                                                                        raw=False,
                                                                        aggregate=True)
                                 for col_name, name in column_mapping.items()} if not overall_only else {}
                                )
             for tag in tags]
        return _router

    def upload_overall(df):
        for col_name, metric in _overall.items():
            df['value'] = df[col_name]
            metric.put(df)

    def upload_df(df):
        router = get_router(df.tag.unique().tolist())
        if len(router) > 0:
            for tag, df_tagged in df.groupby('tag'):
                for col_name, metric in router[tag].items():
                    df_tagged['value'] = df_tagged[col_name]
                    metric.put(df_tagged)
        upload_overall(df)

    return upload_overall if overall_only else upload_df


def main():
    parser = argparse.ArgumentParser(description='Process phantom output.')
    parser.add_argument('phout', type=str, help='path to phantom output file')
    parser.add_argument('--url', type=str, default='https://volta-back-testing.common-int.yandex-team.ru/')
    parser.add_argument('--name', type=str, help='test name', default=str(datetime.utcnow()))
    parser.add_argument('--db_name', type=str, help='ClickHouse database name', default='luna_test')
    args = parser.parse_args()

    clients = [{'type': 'luna', 'api_address': args.url, 'db_name': args.db_name}]
    data_session = DataSession({'clients': clients})
    data_session.update_job({'name': args.name})
    print('Test name: %s' % args.name)
    # col_map = {name: (name, 'fractions') for name in ['connect_time', 'send_time',
    #            'latency', 'receive_time',
    #            'interval_event']}
    metrics_map = {
        'interval_real', 'connect_time', 'send_time', 'latency',
                   'receive_time', 'interval_event', 'net_code', 'proto_code'}
    col_map_aggr = {name: 'metric %s' % name for name in
                    ['interval_real', 'connect_time', 'send_time', 'latency',
                     'receive_time', 'interval_event']}
    uploader = get_uploader(data_session, col_map_aggr, True)

    signal.signal(signal.SIGINT, get_handler(data_session))

    with open(args.phout) as f:
        buffer = ''
        while True:
            parts = f.read(128*1024)
            try:
                chunk, new_buffer = parts.rsplit('\n', 1)
                chunk = buffer + chunk + '\n'
                buffer = new_buffer
            except ValueError:
                chunk = buffer + parts
                buffer = ''
            if len(chunk) > 0:
                df = string_to_df_microsec(chunk)
                uploader(df)
            else:
                break
    data_session.close()

    # <type 'list'>: ['848f1769e16843f49d4f1b5b43c26124', '700b9fbb626c492b8fe16793ba659561', '4c22258a07984acfaf92d67e13c050a2', '1c3f1af845b842e7ae63554f74c89661', '17b8fd1da726452ba6abb9261a09d53f']

if __name__ == '__main__':
    main()