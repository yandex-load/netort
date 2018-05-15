from ..common.interfaces import AbstractMetric
import numpy as np


class Aggregate(AbstractMetric):
    def __init__(self, meta, queue):
        super(Aggregate, self).__init__(meta, queue)
        self.dtypes = None  # TODO FIXME unknown right now
        self.columns = None  # TODO FIXME unknown right now

    @property
    def type(self):
        return 'aggregates'
