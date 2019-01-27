from Queue import Queue

import pandas as pd
from netort.data_manager.common.interfaces import MetricData
from netort.data_manager.metrics import Aggregate
import pytest


def test_value_asserion():
    aggr_metric = Aggregate({'type': Aggregate.type}, Queue())
    data = pd.DataFrame.from_csv('netort/data_manager/metrics/tests/df1.csv')
    with pytest.raises(AssertionError) as e:
        aggr_metric.put(data)
    assert e


@pytest.mark.parametrize('input_file, output_file', [
    ('netort/data_manager/metrics/tests/df1.csv', 'netort/data_manager/tests/df1MetricData.csv'),
    ('netort/data_manager/metrics/tests/df1.csv', 'netort/data_manager/tests/df2MetricData.csv')
])
def test_put(input_file, output_file):
    q = Queue()
    aggr_metric = Aggregate({'type': Aggregate.type}, q)
    data = pd.DataFrame.from_csv(input_file)
    data['value'] = data['interval_real']
    aggr_metric.put(data)
    res = q.get()
    assert isinstance(res, MetricData) #expected
    assert all(res.df.columns == ['ts', 'value'])
    assert all(res == pd.DataFrame.from_csv(output_file))