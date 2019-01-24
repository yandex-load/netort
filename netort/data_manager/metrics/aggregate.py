from collections import Counter

from ..common.interfaces import AbstractMetric
import numpy as np
import pandas as pd




class Aggregator(object):
    perc_list = [0, 10, 25, 50, 75, 80, 85, 90, 95, 98, 99, 100]
    percentiles = np.array(perc_list)

    @classmethod
    def aggregate(cls, by_second):
        result = pd.DataFrame.from_dict({ts: self.aggregates(df) for ts, df in by_second.items()}
                                        , orient='index', columns=Aggregate.columns)
        return df

    @staticmethod
    def _mean(series):
        return series.mean().item()

    @staticmethod
    def _std(series):
        return series.std()

    @staticmethod
    def _len(series):
        return len(series)

    @classmethod
    def _quantiles(cls, series):
        return {
            "q": list(cls.percentiles),
            "value": list(np.percentile(series, cls.percentiles)),
        }

AGGREGATORS = {
    "q":Aggregator._quantiles,
    "average":Aggregator._mean,
    "stddev":Aggregator._std,
    "len":Aggregator._len,
}

class Aggregate(AbstractMetric):
    qlist = ['q%d'%n for n in Aggregator.perc_list]
    columns = ['ts'] + qlist + ['average', 'stddev']

    def __init__(self, meta, queue):
        super(Aggregate, self).__init__(meta, queue)
        self.dtypes = {
            'ts': np.int64,
            'average': np.float64,
            'stddev': np.float64,
        }
        for q in self.qlist:
            self.dtypes[q] = np.float64

    @property
    def type(self):
        return 'aggregates'
