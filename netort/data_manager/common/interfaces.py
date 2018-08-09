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


class AbstractMetric(object):
    def __init__(self, meta, queue_):
        self.local_id = "metric_{uuid}".format(uuid=uuid.uuid4())
        self.dtypes = {}
        self.meta = meta
        self.routing_queue = queue_
        self.columns = []
        self.tag = None

    @property
    def type(self):
        raise NotImplementedError('Abstract type property should be redefined!')

    def put(self, df):
        # FIXME assert w/ dtypes here ?
        df['type'] = self.type
        df['metric_local_id'] = self.local_id
        df = df.set_index('metric_local_id')
        self.routing_queue.put((df, self.type))
