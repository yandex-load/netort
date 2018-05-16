from ..common.interfaces import AbstractMetric
import numpy as np


class Distribution(AbstractMetric):
    def __init__(self, meta, queue):
        super(Distribution, self).__init__(meta, queue)
        self.dtypes = {
            'ts': np.int64,
            'l': np.int64,
            'r': np.int64,
            'cnt': np.int64
        }
        self.columns = ['ts', 'l', 'r', 'cnt']

    @property
    def type(self):
        return 'distributions'
