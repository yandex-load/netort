import queue
import uuid


class AbstractClient(object):
    def __init__(self, meta, job):
        self.local_id = "client_{uuid}".format(uuid=uuid.uuid4())
        self.pending_metrics = []
        self.job = job
        self.pending_queue = queue.Queue()
        self.meta = meta

    def subscribe(self, metric):
        self.pending_metrics.append(metric)

    def put(self, df):
        self.pending_queue.put(df)

    def update_job(self, meta):
        pass

    def update_metric(self, meta):
        pass


class MetricData(object):
    def __init__(self, df, _type, local_id):
        df['metric_local_id'] = local_id
        df = df.set_index('metric_local_id')
        self.df = df
        self.type = _type
        self.local_id = local_id


class AbstractMetric(object):
    VALUE_COL = 'value'
    TS_COL = 'ts'

    def __init__(self, meta, queue_):
        self.local_id = "metric_{uuid}".format(uuid=uuid.uuid4())
        self.dtypes = {}
        self.meta = meta
        self.routing_queue = queue_
        self.tag = None

    @property
    def type(self):
        raise NotImplementedError('Abstract type property should be redefined!')

    def put(self, df):
        # FIXME check dtypes of an incoming dataframe
        # df['type'] = self.type
        # df['metric_local_id'] = self.local_id
        # df = df.set_index('metric_local_id')
        data = MetricData(df, self.type, self.local_id)
        self.routing_queue.put(data)

