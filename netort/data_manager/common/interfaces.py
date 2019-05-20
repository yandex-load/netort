# coding=utf-8
from collections import Counter

import pandas as pd
import queue
import uuid
import numpy as np


class Aggregated(object):
    buffer_size = 10  # seconds

    @classmethod
    def is_aggregated(cls):
        return True


class DataType(object):
    table_name = ''
    columns = []
    is_aggregated = False

    @classmethod
    def processor(cls, df, last_piece):
        """
        :type df: pandas.DataFrame
        :type last_piece: bool
        :rtype: pandas.DataFrame
        """
        return df

    @classmethod
    def is_aggregated(cls):
        return False


class TypeTimeSeries(DataType):
    table_name = 'metrics'
    columns = ['ts', 'value']


class TypeEvents(DataType):
    table_name = 'events'
    columns = ['ts', 'value']


class TypeQuantiles(Aggregated, DataType):
    perc_list = [0, 10, 25, 50, 75, 80, 85, 90, 95, 98, 99, 100]
    qlist = ['q%d' % n for n in perc_list]
    rename = {'mean': 'average', 'std': 'stddev', '0%': 'q0', '10%': 'q10', '25%': 'q25',
              '50%': 'q50', '75%': 'q75', '80%': 'q80', '85%': 'q85', '90%': 'q90',
              '95%': 'q95', '98%': 'q98', '99%': 'q99', '100%': 'q100', }

    table_name = 'aggregates'
    columns = ['ts'] + qlist + ['average', 'stddev']
    __aggregator_buffer = {}
    aggregator_buffer_size = 10

    @classmethod
    def processor(cls, df, last_piece, groupby='second'):
        # result = pd.DataFrame.from_dict({ts: self.aggregates(df) for ts, df in by_second.items()}
        #                                 , orient='index', columns=Aggregate.columns)
        df = df.set_index(groupby)
        series = df.loc[:, AbstractMetric.VALUE_COL]
        res = series.groupby(series.index). \
            describe(percentiles=[i / 100. for i in cls.perc_list]). \
            rename(columns=cls.rename)
        res['ts'] = res.index
        return res


class TypeDistribution(Aggregated, DataType):
    table_name = 'distributions'
    columns = ['ts', 'l', 'r', 'cnt']
    DEFAULT_BINS = np.concatenate((
        np.linspace(0, 4990, 500, dtype=int),  # 10µs accuracy
        np.linspace(5000, 9900, 50, dtype=int),  # 100µs accuracy
        np.linspace(10, 499, 490, dtype=int) * 1000,  # 1ms accuracy
        np.linspace(500, 2995, 500, dtype=int) * 1000,  # 5ms accuracy
        np.linspace(3000, 9990, 700, dtype=int) * 1000,  # 10ms accuracy
        np.linspace(10000, 29950, 400, dtype=int) * 1000,  # 50ms accuracy
        np.linspace(30000, 119900, 900, dtype=int) * 1000,  # 100ms accuracy
        np.linspace(120, 300, 181, dtype=int) * 1000000  # 1s accuracy
    ))

    @classmethod
    def processor(cls, df, last_piece, bins=DEFAULT_BINS, groupby='second'):
        df = df.set_index(groupby)
        series = df.loc[:, AbstractMetric.VALUE_COL]
        data = {ts: np.histogram(s, bins=bins) for ts, s in series.groupby(series.index)}
        # data, bins = np.histogram(series, bins=bins)
        result = pd.concat(
            [pd.DataFrame.from_dict(
                {'l': bins[:-1],
                 'r': bins[1:],
                 'cnt': data,
                 'ts': ts}
            ).query('cnt > 0') for ts, (data, bins) in data.items()]
        )
        return result


class TypeHistogram(Aggregated, DataType):
    table_name = 'histograms'
    columns = ['ts', 'category', 'cnt']

    @classmethod
    def processor(cls, df, last_piece, groupby='second'):
        df = df.set_index(groupby)
        series = df.loc[:, AbstractMetric.VALUE_COL]
        data = series.groupby([series.index, series.values]).size().reset_index().\
            rename(columns={'second':'ts', 'level_1': 'category', 'value': 'cnt'})
        return data


class AbstractClient(object):
    def __init__(self, meta, job):
        self.local_id = "client_{uuid}".format(uuid=uuid.uuid4())
        self.pending_metrics = []
        self.job = job
        self.pending_queue = queue.Queue()
        self.meta = meta

    def subscribe(self, metric):
        self.pending_metrics.append(metric)

    def put(self, data_type, df):
        self.pending_queue.put((data_type, df))

    def update_job(self, meta):
        pass

    def update_metric(self, meta):
        pass


class MetricData(object):
    def __init__(self, df, data_types, local_id):
        """

        :param df: pandas.DataFrame
        :param data_types: list of DataType
        :param local_id: uuid4
        """
        df['metric_local_id'] = local_id
        df = df.set_index('metric_local_id')
        self.data_types = data_types
        self.local_id = local_id
        if self.is_aggregated:
            df['second'] = (df['ts'] / 1000000).astype(int)
        self.df = df

    @property
    def is_aggregated(self):
        return any([dtype.is_aggregated() for dtype in self.data_types])


class AbstractMetric(object):
    VALUE_COL = 'value'
    TS_COL = 'ts'

    def __init__(self, meta, queue_, raw=True, aggregate=False):
        self.local_id = "metric_{uuid}".format(uuid=uuid.uuid4())
        self.meta = meta
        self.routing_queue = queue_
        self.raw = raw
        self.aggregate = aggregate
        if not (raw or aggregate):
            raise ValueError('Either raw or aggregate must be True to upload some data')

    @property
    def type(self):
        """
        :rtype: DataType
        """
        raise NotImplementedError('Abstract type property should be redefined!')

    @property
    def aggregate_types(self):
        """
        :rtype: list of DataType
        """
        raise NotImplementedError('Abstract type property should be redefined!')

    @property
    def data_types(self):
        """
        :rtype: list of DataType
        """
        return [self.type] * self.raw + self.aggregate_types * self.aggregate

    def put(self, df):
        # FIXME check dtypes of an incoming dataframe
        # df['type'] = self.type
        # df['metric_local_id'] = self.local_id
        # df = df.set_index('metric_local_id')
        data = MetricData(df, self.data_types, self.local_id)
        self.routing_queue.put(data)