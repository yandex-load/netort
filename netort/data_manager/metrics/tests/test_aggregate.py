try:
    from Queue import Queue
except ImportError:
    from queue import Queue

import pandas as pd
from netort.data_manager.common.interfaces import MetricData
from netort.data_manager.metrics import Aggregate
import pytest
from netort.data_manager.metrics.aggregate import Aggregator


def test_value_asserion():
    aggr_metric = Aggregate({'type': Aggregate.type}, parent=None, queue=Queue())
    data = pd.read_csv('netort/data_manager/metrics/tests/df1.csv')
    with pytest.raises(AssertionError) as e:
        aggr_metric.put(data)
    assert e


@pytest.mark.parametrize('input_file, output_file', [
    ('netort/data_manager/metrics/tests/df1.csv', 'netort/data_manager/tests/df1MetricData.csv'),
    ('netort/data_manager/metrics/tests/df1.csv', 'netort/data_manager/tests/df2MetricData.csv')
])
def test_put(input_file, output_file):
    q = Queue()
    aggr_metric = Aggregate({'type': Aggregate.type}, None, q)
    data = pd.read_csv(input_file)
    data['value'] = data['interval_real']
    aggr_metric.put(data)
    res = q.get()
    assert isinstance(res, MetricData) #expected
    assert all(res.df.columns == ['ts', 'value'])
    assert all(res == pd.DataFrame.from_csv(output_file))


def test_aggregator():
    data = pd.read_csv('netort/data_manager/metrics/tests/df1_buffered.csv')
    aggregated = Aggregator.aggregate(data)
    assert all([col in aggregated.columns for col in Aggregate.columns])