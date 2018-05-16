from ..common.interfaces import AbstractMetric
import numpy as np


class Histogram(AbstractMetric):
    def __init__(self, meta, queue):
        super(Histogram, self).__init__(meta, queue)
        self.dtypes = {
            'ts': np.int64,
            'category': np.str,
            'cnt': np.int64
        }
        self.columns = ['ts', 'category', 'cnt']

    @property
    def type(self):
        return 'histograms'
