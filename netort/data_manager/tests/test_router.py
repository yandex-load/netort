import pytest
from mock import Mock
from netort.data_manager import MetricsRouter, DataManager
import pandas as pd


class TestAggregatorBuffer(object):

    def setup_method(self):
        self.metrics_router = MetricsRouter(Mock(DataManager), 5)

    def test_buffer_last_piece(self):
        df1 = pd.DataFrame.from_csv('netort/data_manager/tests/df1.csv')
        df2 = pd.DataFrame.from_csv('netort/data_manager/tests/df2.csv')
        res1 = self.metrics_router._MetricsRouter__from_aggregator_buffer(df1, 'metric1', False)
        res2 = self.metrics_router._MetricsRouter__from_aggregator_buffer(df2, 'metric1', True)
        assert len(df1) + len(df2) == len(res1) + len(res2)

    def test_buffer_no_last_piece(self):
        df1 = pd.DataFrame.from_csv('netort/data_manager/tests/df1.csv')
        df2 = pd.DataFrame.from_csv('netort/data_manager/tests/df2.csv')
        res1 = self.metrics_router._MetricsRouter__from_aggregator_buffer(df1, 'metric1', False)
        res2 = self.metrics_router._MetricsRouter__from_aggregator_buffer(df2, 'metric1', False)
        assert len(df1) + len(df2) > len(res1) + len(res2)
        assert len(df1) + len(df2) == len(res1) + len(res2) +\
                                      len(self.metrics_router._MetricsRouter__aggregator_buffer.get('metric1', []))

    def test_buffer_multiple_metrics(self):
        df1 = pd.DataFrame.from_csv('netort/data_manager/tests/df1.csv')
        df2 = pd.DataFrame.from_csv('netort/data_manager/tests/df2.csv')
        res11 = self.metrics_router._MetricsRouter__from_aggregator_buffer(df1, 'metric1', False)
        res21 = self.metrics_router._MetricsRouter__from_aggregator_buffer(df1, 'metric2', False)
        res12 = self.metrics_router._MetricsRouter__from_aggregator_buffer(df2, 'metric1', False)
        res22 = self.metrics_router._MetricsRouter__from_aggregator_buffer(df2, 'metric2', True)
        assert len(df1) + len(df2) == len(res11) + len(res12) + \
                                      len(self.metrics_router._MetricsRouter__aggregator_buffer.get('metric1', []))
        assert len(df1) + len(df2) == len(res21) + len(res22)
        assert self.metrics_router._MetricsRouter__aggregator_buffer.get('metric2', []) is None