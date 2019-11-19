import pandas as pd
import six
from netort.data_manager import Metric

if six.PY3:
    from queue import Queue
else:  # six.PY2
    from Queue import Queue

from netort.data_manager.common.interfaces import TypeQuantiles, TypeHistogram, TypeDistribution
import pytest


@pytest.mark.xfail
def test_processor():
    data = pd.read_csv('netort/data_manager/metrics/tests/df1_buffered.csv')
    aggregated = TypeQuantiles.processor(data, True)
    assert all([col in aggregated.columns for col in TypeQuantiles.columns])


def test_histograms_processor():
    data = pd.read_csv('netort/data_manager/metrics/tests/metric_data_input_event_1.csv')
    data.loc[:, 'second'] = (data['ts'] / 1000000).astype(int)
    expected = pd.read_csv('netort/data_manager/metrics/tests/metric_data_output_histogram_1.csv')
    aggregated = TypeHistogram.processor(data)
    assert expected.equals(aggregated)


def test_quantiles_processor():
    data = pd.read_csv('netort/data_manager/metrics/tests/metric_data_input_metric_2.csv')
    data.loc[:, 'second'] = (data['ts'] / 1000000).astype(int)
    expected = pd.read_csv('netort/data_manager/metrics/tests/metric_data_output_quantile_2.csv')
    expected = expected.round(2).set_index('second')
    aggregated = TypeQuantiles.processor(data).round(2)
    assert aggregated.equals(expected)


def test_distributions_processor():
    data = pd.read_csv('netort/data_manager/metrics/tests/metric_data_input_metric_2.csv')
    data.loc[:, 'second'] = (data['ts'] / 1000000).astype(int)
    aggregated = TypeDistribution.processor(data).round(2)
    expected = pd.read_csv('netort/data_manager/metrics/tests/metric_data_output_distributions_2.csv').set_index('second')
    assert aggregated.equals(expected)

