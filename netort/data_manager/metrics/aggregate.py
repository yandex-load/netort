from collections import Counter

from ..common.interfaces import AbstractMetric
import numpy as np
import pandas as pd


class Aggregator(object):
    perc_list = [0, 10, 25, 50, 75, 80, 85, 90, 95, 98, 99, 100]
    percentiles = np.array(perc_list)
    rename = {
        'mean': 'average',
        'std': 'stddev',
        '0%': 'q0',
        '10%': 'q10',
        '25%': 'q25',
        '50%': 'q50',
        '75%': 'q75',
        '80%': 'q80',
        '85%': 'q85',
        '90%': 'q90',
        '95%': 'q95',
        '98%': 'q98',
        '99%': 'q99',
        '100%': 'q100',
        }

    @classmethod
    def aggregate(cls, df, groupby='second'):
        # result = pd.DataFrame.from_dict({ts: self.aggregates(df) for ts, df in by_second.items()}
        #                                 , orient='index', columns=Aggregate.columns)
        df = df.set_index(groupby)
        series = df.loc[:, AbstractMetric.VALUE_COL]
        res = series.groupby(series.index).\
            describe(percentiles=[i / 100. for i in cls.perc_list]).\
            rename(columns=cls.rename)
        res['ts'] = res.index
        return res


class Aggregate(AbstractMetric):
    qlist = ['q%d'%n for n in Aggregator.perc_list]
    columns = ['ts'] + qlist + ['average', 'stddev']
    type = 'aggregates'

    def __init__(self, meta, queue):
        super(Aggregate, self).__init__(meta, queue)
        self.dtypes = {
            'ts': np.int64,
            'average': np.float64,
            'stddev': np.float64,
        }
        for q in self.qlist:
            self.dtypes[q] = np.float64
        self._start_ts = None

    def put(self, df):
        if self._start_ts is None:
            self._start_ts = df["ts"].iloc[0]
        df["ts"] -= self._start_ts
        assert AbstractMetric.TS_COL in df.columns
        assert AbstractMetric.VALUE_COL in df.columns
        super(Aggregate, self).put(df.loc[:, [AbstractMetric.TS_COL, AbstractMetric.VALUE_COL]])
