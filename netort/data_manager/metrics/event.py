from ..common.interfaces import AbstractMetric, TypeEvents, TypeHistogram
import numpy as np


class Event(AbstractMetric):
    def __init__(self, meta, queue, test_start, raw=True, aggregate=False, **kw):
        super(Event, self).__init__(meta, queue, test_start, raw, aggregate, **kw)
        self.dtypes = {
            'ts': np.int64,
            'value': np.str,
        }
        self.columns = ['ts', 'value']

    @property
    def type(self):
        return TypeEvents

    @property
    def aggregate_types(self):
        return [TypeHistogram]
