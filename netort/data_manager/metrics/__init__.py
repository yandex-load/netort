from .metric import Metric
from .aggregate import Aggregate
from .histogram import Histogram
from .event import Event
from .distribution import Distribution


available_metrics = {
    'metrics': Metric,
    'aggregates': Aggregate,
    'histograms': Histogram,
    'events': Event,
    'distributions': Distribution
}