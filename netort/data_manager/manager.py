# TODO: move code that works with config to library's clients (e.g., Volta). Classes of this library should provide constructors with described arguments only
import logging
import uuid
import time
import os
import getpass
import six
from netort.data_manager.metrics import Metric, Event

if six.PY3:
    from queue import Queue
else:  # six.PY2
    from Queue import Queue

import pandas as pd

from .clients import available_clients
from .router import MetricsRouter

import warnings
# FIXME: this one is dangerous because it ignores all FutureWarnings, not only required one
warnings.filterwarnings("ignore", category=FutureWarning)  # pandas sorting warnings


logger = logging.getLogger(__name__)


class DataSession(object):
    """
    Workflow:
        * create DataSession object
        * use `new_metric` to add metrics to your datasession
        * use `metric.put` to add data to the metric
        * call `close` to close your datasession

    Note:
        * send your data in chunks because it could be of bigger size that server's buffer

    Args:
        config(dict): configuration options (list of DataManager clients, test meta data etc)

    TODO:
        * move config parameters to kwargs, describe them here
        * chunkify data for upload inside the uploader code
        * fight performance issues (probably caused by poor pandas write_csv performance)
    """
    def __init__(self,  config):
        self.config = config
        self.operator = self.__get_operator()
        self.job_id = config.get('test_id', 'job_{uuid}'.format(uuid=uuid.uuid4()))
        logger.info('Created new local data session: %s', self.job_id)
        self.test_start = config.get('test_start', int(time.time() * 10**6))
        self.artifacts_base_dir = config.get('artifacts_base_dir', './logs')
        self._artifacts_dir = None
        self.manager = DataManager()

        self.clients = []
        self.__create_clients(config.get('clients', []))
        logger.debug('DataSession clients: %s', self.clients)
        logger.debug('DataSession subscribers: %s', self.manager.subscribers)

    # TODO: extract client creation as factory method
    # TODO: consider removing clients from config and add them via `new_client` method
    def __create_clients(self, clients):
        for client_meta in clients:
            type_ = client_meta.get('type')
            filter_ = client_meta.get('filter', {'type': '__ANY__'})
            if not type_:
                raise ValueError('Client type should be defined.')
            if type_ in available_clients:
                client = available_clients[type_](client_meta, self)
                self.subscribe(client.put, filter_)
                self.clients.append(client)
            else:
                raise NotImplementedError('Unknown client type: %s' % type_)

    def new_true_metric(self, name, raw=True, aggregate=False, **kw):
        return self.manager.new_true_metric(name, raw, aggregate, **kw)

    def new_event_metric(self, name, raw=True, aggregate=False, **kw):
        return self.manager.new_event_metric(name, raw, aggregate, **kw)

    def subscribe(self, callback, filter_):
        return self.manager.subscribe(callback, filter_)

    def get_metric_by_id(self, id_):
        return self.manager.get_metric_by_id(id_)

    def update_job(self, meta):
        for client in self.clients:
            try:
                client.update_job(meta)
            except Exception:
                logger.warning('Client %s job update failed', client)
                logger.debug('Client %s job update failed', client, exc_info=True)
            else:
                logger.debug('Client job updated: %s', client)

    def update_metric(self, meta):
        for client in self.clients:
            try:
                client.update_metric(meta)
            except Exception:
                logger.warning('Client %s metric update failed', client)
                logger.debug('Client %s metric update failed', client, exc_info=True)
            else:
                logger.debug('Client metric updated: %s', client)

    # TODO: artifacts dir should be inside "local" client. Or does it?
    @property
    def artifacts_dir(self):
        if not self._artifacts_dir:
            dir_name = "{dir}/{id}".format(dir=self.artifacts_base_dir, id=self.job_id)
            if not os.path.isdir(dir_name):
                os.makedirs(dir_name)
            os.chmod(dir_name, 0o755)
            self._artifacts_dir = os.path.abspath(dir_name)
        return self._artifacts_dir

    def __get_operator(self):
        try:
            return self.config.get('operator') or getpass.getuser()
        except:  # noqa: E722
            logger.error(
                "Couldn't get username from the OS. Please, set the 'operator' option explicitly in your config "
                "file.")
            raise

    def close(self):
        logger.info('DataSession received close signal.')
        logger.info('Closing DataManager')
        self.manager.close()
        logger.info('Waiting the rest of data from router...')
        self.manager.router.join()
        logger.info('Sending close to DataSession clients...')
        for client in self.clients:
            try:
                client.close()
            except Exception:
                logger.warning('Client %s failed to close', client, exc_info=True)
            else:
                logger.debug('Client closed: %s', client)
        logger.info('DataSession finished!')

    def interrupt(self):
        self.manager.interrupt()
        for client in self.clients:
            try:
                client.close()
            except Exception:
                logger.warning('Client %s failed to close', client)
            else:
                logger.debug('Client closed: %s', client)
        logger.info('DataSession finished!')


class DataManager(object):
    """DataManager routes data to subscribers using metrics meta as a filter. When someone calls
    `new_metric`, DataManager will find the subscribers that are interested in this metric (using meta).
    When someone calls `subscribe`, DataManager finds the metrics that this subscriber is interested in.

    MetricsRouter is a facility that DataManager uses for passing incoming data to subscribers.

    Attributes:
        metrics (dict): All registered metrics for DataManager session
        subscribers (pd.DataFrame): All registered subscribers for DataManager session
        callbacks (pd.DataFrame): callbacks for metric ids <-> subscribers' callbacks, used by router
        routing_queue (Queue): incoming unrouted metrics data,
            will be processed by MetricsRouter to subscribers' callbacks
        router (MetricsRouter object): Router thread. Read routing queue, concat incoming messages by metrics.type,
            left join by callback and call callback w/ resulting dataframe
    """
    def __init__(self):
        self.metrics = {}
        self.metrics_meta = pd.DataFrame(columns=['type'])
        self.subscribers = pd.DataFrame(columns=['type'])
        self.callbacks = pd.DataFrame(columns=['id', 'callback'])
        self.routing_queue = Queue()
        self.router = MetricsRouter(self)
        self.router.start()

    def new_true_metric(self, name, raw=True, aggregate=False, **kw):
        """
        Create and register metric,
        find subscribers for this metric (using meta as filter) and subscribe

        Return:
            metric (available_metrics[0]): one of Metric
        """
        return self._new_metric(Metric, raw, aggregate, name=name, **kw)

    def new_event_metric(self, name, raw=True, aggregate=False, **kw):
        return self._new_metric(Event, raw, aggregate, name=name, **kw)

    def _new_metric(self, dtype, raw=True, aggregate=False, **kw):

        metric_obj = dtype(kw, self.routing_queue, raw=raw, aggregate=aggregate)  # create metric object
        metric_meta = pd.DataFrame({metric_obj.local_id: kw}).T  # create metric meta
        self.metrics_meta.append(metric_meta)  # register metric meta
        self.metrics[metric_obj.local_id] = metric_obj  # register metric object

        # find subscribers for this metric
        this_metric_subscribers = self.__reversed_filter(self.subscribers, kw)
        if this_metric_subscribers.empty:
            logger.debug('subscriber for metric %s not found', metric_obj.local_id)
        else:
            logger.debug('Found subscribers for this metric, subscribing...: %s', this_metric_subscribers)
            # attach this metric id to discovered subscribers and select id <-> callbacks
            this_metric_subscribers['id'] = metric_obj.local_id
            found_callbacks = this_metric_subscribers[['id', 'callback']].set_index('id')
            # add this metric callbacks to DataManager's callbacks
            self.callbacks = self.callbacks.append(found_callbacks)
        return metric_obj

    def subscribe(self, callback, filter_):
        """
        Create and register metric subscriber,
        find metrics for this subscriber (using filter_) and subscribe

        Args:
            callback (object method): subscriber's callback
            filter_ (dict): filter dict

        filter sample:
            {'type': 'metrics', 'source': 'gun'}
        """
        sub_id = "subscriber_{uuid}".format(uuid=uuid.uuid4())
        # register subscriber in manager
        sub = pd.DataFrame({sub_id: filter_}).T
        sub['callback'] = callback
        self.subscribers = self.subscribers.append(sub)

        # find metrics for subscriber using `filter`
        this_subscriber_metrics = self.__filter(self.metrics_meta, filter_)
        if this_subscriber_metrics.empty:
            logger.debug('Metrics for subscriber %s not found', sub_id)
        else:
            logger.debug('Found metrics for this subscriber, subscribing...: %s', this_subscriber_metrics)
            # attach this sub callback to discovered metrics and select id <-> callbacks
            this_subscriber_metrics['callback'] = callback
            prepared_callbacks = this_subscriber_metrics[['callback']]
            # add this subscriber callbacks to DataManager's callbacks
            self.callbacks = self.callbacks.append(prepared_callbacks)

    def get_metric_by_id(self, id_):
        return self.metrics.get(id_)

    @staticmethod
    def __filter(filterable, filter_, logic_operation='and'):
        """ filtering DataFrame using filter_ key-value conditions applying logic_operation
        only find rows strictly fitting the filter_ criterion"""
        condition = []
        if not filter_:
            return filterable
        elif filter_.get('type') == '__ANY__':
            return filterable
        else:
            for key, value in filter_.items():
                condition.append('{key} == "{value}"'.format(key=key, value=value))
        try:
            res = filterable.query(" {operation} ".format(operation=logic_operation).join(condition))
        except pd.core.computation.ops.UndefinedVariableError:
            return pd.DataFrame()
        else:
            return res

    @staticmethod
    def __reversed_filter(filterable, filter_, logic_operation='and'):
        """ reverse filtering DataFrame using filter_ key-value conditions applying logic_operation
        find rows where existing filterable columns (and its values) fitting the filter_ criterion"""
        condition = []
        try:
            subscribers_for_any = filterable.query('type == "__ANY__"')
        except pd.core.computation.ops.UndefinedVariableError:
            subscribers_for_any = pd.DataFrame()
        if not filter_:
            return filterable
        else:
            for existing_col in filterable:
                for meta_tag, meta_value in filter_.items():
                    if meta_tag == existing_col:
                        condition.append('{key} == "{value}"'.format(key=meta_tag, value=meta_value))
            try:
                res = filterable.query(" {operation} ".format(operation=logic_operation).join(condition))
            except (pd.core.computation.ops.UndefinedVariableError, ValueError):
                return pd.DataFrame().append(subscribers_for_any)
            else:
                return res.append(subscribers_for_any)

    def close(self):
        self.router.close()

    def interrupt(self):
        self.router.interrupt()
        self.router.join()


def usage_sample():
    import time
    import pandas as pd
    config = {
        'clients': [
            {
                'type': 'luna',
                'api_address': 'http://hostname.tld',
                'user_agent': 'Tank Test',
            },
            {
                'type': 'local_storage',
            }
        ],
        'test_start': time.time(),
        'artifacts_base_dir': './logs'
    }
    data_session = DataSession(config=config)

    metric_meta = {
        'type': 'metrics',
        'name': 'cpu_usage',
        'hostname': 'localhost',
        'some_meta_key': 'some_meta_value'
    }

    metric_obj = data_session.new_true_metric('name', **metric_meta)
    time.sleep(1)
    df = pd.DataFrame([[123, 123.123, "trash"]], columns=['ts', 'value', 'trash'])
    metric_obj.put(df)
    df2 = pd.DataFrame([[456, 456.456]], columns=['ts', 'value'])
    metric_obj.put(df2)
    time.sleep(10)
    df = pd.DataFrame([[123, 123.123]], columns=['ts', 'value'])
    metric_obj.put(df)
    df2 = pd.DataFrame([[456, 456.456]], columns=['ts', 'value'])
    metric_obj.put(df2)
    time.sleep(10)
    data_session.close()


if __name__ == '__main__':
    logging.basicConfig(level='DEBUG')
    logger = logging.getLogger(__name__)
    usage_sample()
