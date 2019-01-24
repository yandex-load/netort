import pytest
from mock import Mock
from netort.data_manager import MetricsRouter, DataManager


class TestAggregatorBuffer(object):

    @classmethod
    def setup_class(cls):
        cls.metrics_router = MetricsRouter(Mock(DataManager), 5)

    def test_buffer(self):
        assert self.metrics_router._MetricsRouter__from_aggregator_buffer(df)