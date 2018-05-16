from ..common.interfaces import AbstractMetric
import numpy as np


class Aggregate(AbstractMetric):
    def __init__(self, meta, queue):
        super(Aggregate, self).__init__(meta, queue)
        self.dtypes = {
            'ts': np.int64,
            'average': np.float64,
            'stddev': np.float64,
        }
        self.qlist = ['q0', 'q10', 'q25', 'q50', 'q75', 'q80', 'q85', 'q90', 'q95', 'q98', 'q99', 'q100']
        for q in self.qlist:
            self.dtypes[q] = np.float64
        self.columns = ['ts'] + self.qlist + ['average', 'stddev']

    @property
    def type(self):
        return 'aggregates'
