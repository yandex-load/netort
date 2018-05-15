from ..common.interfaces import AbstractMetric
import numpy as np


class Event(AbstractMetric):
    def __init__(self, meta, queue):
        super(Event, self).__init__(meta, queue)
        self.dtypes = {
            'ts': np.int64,
            'value': np.str,
        }
        self.columns = ['ts', 'value']

    @property
    def type(self):
        return 'events'
