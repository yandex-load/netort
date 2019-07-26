import pandas as pd
import six
from netort.data_manager import Metric

if six.PY3:
    from queue import Queue
else:  # six.PY2
    from Queue import Queue

from netort.data_manager.common.interfaces import TypeQuantiles
import pytest


def test_processor():
    data = pd.read_csv('netort/data_manager/metrics/tests/df1_buffered.csv')
    aggregated = TypeQuantiles.processor(data, True)
    assert all([col in aggregated.columns for col in TypeQuantiles.columns])