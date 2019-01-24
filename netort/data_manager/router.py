import threading
import time
import pandas as pd
import logging

from netort.data_manager.manager import DataManager
from netort.data_manager.metrics import Aggregate

from ..data_processing import get_nowait_from_queue

logger = logging.getLogger(__name__)


class MetricsRouter(threading.Thread):
    """
    Drain incoming queue, concatenate dataframes by metric type and process to callbacks
    callback receives resulting dataframe
    """

    # TODO: MetricsRouter should not know anything about DataManager. Pass source and subscribers directly.
    def __init__(self, manager, aggregator_buffer_size=5):
        """
        :param aggregator_buffer_size: seconds
        :type aggregator_buffer_size: int
        :type manager: DataManager
        """
        super(MetricsRouter, self).__init__()
        self.aggregator_buffer_size = aggregator_buffer_size
        self.manager = manager
        self._finished = threading.Event()
        self._interrupted = threading.Event()
        self.setDaemon(True)  # just in case, bdk+ytank stuck w/o this at join of Drain thread
        self.__aggregator_buffer = {}

    def run(self):
        while not self._interrupted.is_set():
            exec_time_start = time.time()
            self.__route()
            logger.debug('Routing cycle took %.2f ms', (time.time() - exec_time_start) * 1000)
            time.sleep(1)
        logger.info('Router received interrupt signal, routing rest of the data. Qsize: %s',
                    self.manager.routing_queue.qsize())
        self.__route()
        logger.info('Router finished its work')
        self._finished.set()

    def __from_aggregator_buffer(self, df, buff_size=10):
        # type: (pd.DataFrame) -> pd.DataFrame
        metric_id = df['metric_local_id']
        buf = self.__aggregator_buffer.get(metric_id)
        df = df.set_index('ts')
        df['second'] = df.index / 1000000

        if buf is not None:
            df = pd.concat([buf, df])
        cut, new_buf = df[df.second < df.second.max()-buff_size],\
                       df[df.second >= df.second.max()-buff_size]
        self.__aggregator_buffer[metric_id] = new_buf

        grouped = cut.groupby('second')
        a = self.aggregator_buffer_size
        by_second, buf = (lambda l: (dict(l[:-a]), dict(l[-a:])))(sorted(buf.items(), key=lambda x: x[0]))
        return by_second

    def __route(self):
        routing_buffer = {}
        data = get_nowait_from_queue(self.manager.routing_queue)
        for df, type_ in data:
            if type_ == Aggregate.type:
                by_second = self.__from_aggregator_buffer(df, type_)
            if type_ in routing_buffer:
                routing_buffer[type_] = pd.concat([routing_buffer[type_], df], sort=False)
            else:
                routing_buffer[type_] = df

        if self.manager.callbacks.empty:
            logger.debug('No subscribers/callbacks for metrics yet... skipped metrics')
            time.sleep(1)
            return

        # (for each metric type)
        # left join buffer and callbacks, group data by 'callback' then call callback w/ resulting dataframe
        for type_ in routing_buffer:
            try:
                for callback, incoming_chunks in pd.merge(
                        routing_buffer[type_], self.manager.callbacks,
                        how='left',
                        left_index=True,
                        right_index=True
                ).groupby('callback', sort=False):
                    # exec_time_start = time.time()
                    callback(incoming_chunks)
                    # logger.debug('Callback call took %.2f ms', (time.time() - exec_time_start) * 1000)
            except TypeError:
                logger.error('Trash/malformed data sinked into metric type `%s`. Data:\n%s',
                             type_, routing_buffer[type_], exc_info=True)

    def wait(self, timeout=None):
        self._finished.wait(timeout=timeout)

    def close(self):
        self._interrupted.set()
