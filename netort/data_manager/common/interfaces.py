# coding=utf-8
import queue
import uuid
import numpy as np


class DataType(object):
    table_name = ''
    col_names = []

    @classmethod
    def processor(cls, x, **kw):
        return x


class TypeTimeSeries(DataType):
    table_name = 'metrics'
    col_names = ['ts', 'value']


class TypeEvents(DataType):
    table_name = 'events'
    col_names = ['ts', 'value']


class TypeQuantiles(DataType):
    perc_list = [0, 10, 25, 50, 75, 80, 85, 90, 95, 98, 99, 100]
    qlist = ['q%d' % n for n in perc_list]
    rename = {'mean': 'average', 'std': 'stddev', '0%': 'q0', '10%': 'q10', '25%': 'q25',
              '50%': 'q50', '75%': 'q75', '80%': 'q80', '85%': 'q85', '90%': 'q90',
              '95%': 'q95', '98%': 'q98', '99%': 'q99', '100%': 'q100', }

    table_name = 'aggregates'
    col_names = ['ts'] + qlist + ['average', 'stddev']

    @classmethod
    def processor(cls, df, groupby='second'):
        # result = pd.DataFrame.from_dict({ts: self.aggregates(df) for ts, df in by_second.items()}
        #                                 , orient='index', columns=Aggregate.columns)
        df = df.set_index(groupby)
        series = df.loc[:, AbstractMetric.VALUE_COL]
        res = series.groupby(series.index). \
            describe(percentiles=[i / 100. for i in cls.perc_list]). \
            rename(columns=cls.rename)
        res['ts'] = res.index
        return res


class TypeDistribution(DataType):
    table_name = 'distributions'
    col_names = ['ts', 'l', 'r', 'cnt']
    DEFAULT_BINS = np.concatenate((
        np.linspace(0, 4990, 500),  # 10µs accuracy
        np.linspace(5000, 9900, 50),  # 100µs accuracy
        np.linspace(10, 499, 490) * 1000,  # 1ms accuracy
        np.linspace(500, 2995, 500) * 1000,  # 5ms accuracy
        np.linspace(3000, 9990, 700) * 1000,  # 10ms accuracy
        np.linspace(10000, 29950, 400) * 1000,  # 50ms accuracy
        np.linspace(30000, 119900, 900) * 1000,  # 100ms accuracy
        np.linspace(120, 300, 181) * 1000000  # 1s accuracy
    ))

    @classmethod
    def processor(cls, series, bins=DEFAULT_BINS):
        data, bins = np.histogram(series, bins=bins)
        mask = data > 0
        return {
            "data": [e.item() for e in data[mask]],
            "bins": [e.item() for e in bins[1:][mask]],
        }


class TypeHistogram(DataType):
    table_name = 'histograms'
    col_names = ['ts', 'category', 'cnt']

#
# class DataType(Enum):
#     raw_metric = 'metrics'
#     raw_events = 'events'
#     quantiles = 'aggregates'
#     distribution = 'distributions'
#     histograms = 'histograms'


class AbstractClient(object):
    def __init__(self, meta, job):
        self.local_id = "client_{uuid}".format(uuid=uuid.uuid4())
        self.pending_metrics = []
        self.job = job
        self.pending_queue = queue.Queue()
        self.meta = meta

    def subscribe(self, metric):
        self.pending_metrics.append(metric)

    def put(self, df):
        self.pending_queue.put(df)

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
        self.df = df
        self.types = data_types[0]
        self.local_id = local_id


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
        return [self.type] * self.raw + self.aggregate_types * self.aggregate

    def put(self, df):
        # FIXME check dtypes of an incoming dataframe
        # df['type'] = self.type
        # df['metric_local_id'] = self.local_id
        # df = df.set_index('metric_local_id')
        data = MetricData(df, self.data_types, self.local_id)
        self.routing_queue.put(data)
