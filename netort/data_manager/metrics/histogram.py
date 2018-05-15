from ..common.interfaces import AbstractMetric
import numpy as np


class Histogram(AbstractMetric):
    def __init__(self, meta, queue):
        super(Histogram, self).__init__(meta, queue)
        self.dtypes = None  # TODO FIXME unknown right now
        self.columns = None  # TODO FIXME unknown right now

    @property
    def type(self):
        return 'histograms'
