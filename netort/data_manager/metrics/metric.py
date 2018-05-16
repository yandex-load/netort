from ..common.interfaces import AbstractMetric
import numpy as np


class Metric(AbstractMetric):
    def __init__(self, meta, queue):
        super(Metric, self).__init__(meta, queue)
        self.dtypes = {
            'ts': np.int64,
            'value': np.float64
        }
        self.columns = ['ts', 'value']

    @property
    def type(self):
        return 'metrics'
