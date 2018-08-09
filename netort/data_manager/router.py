import threading
import time
import pandas as pd
import logging

from ..data_processing import get_nowait_from_queue

logger = logging.getLogger(__name__)


class MetricsRouter(threading.Thread):
    """
    Drain incoming queue, concatenate dataframes by metric type and process to callbacks
    callback receives resulting dataframe
    """

    def __init__(self, manager):
        super(MetricsRouter, self).__init__()
        self.manager = manager
        self.source = manager.routing_queue
        self._finished = threading.Event()
        self._interrupted = threading.Event()
        self.setDaemon(True)  # just in case, bdk+ytank stuck w/o this at join of Drain thread
        self.routing_buffer = {}

    def run(self):
        while not self._interrupted.is_set():
            exec_time_start = time.time()
            self.__route()
            logger.debug('Routing cycle took %.2f ms', (time.time() - exec_time_start) * 1000)
            if self._interrupted.is_set():
                break
            time.sleep(1)
        logger.info('Router received interrupt signal, routing rest of the data. Qsize: %s', self.source.qsize())
        self.__route()
        logger.info('Router finished its work')
        self._finished.set()

    def __route(self):
        self.routing_buffer = {}
        data = get_nowait_from_queue(self.source)
        for df, type_ in data:
            if type_ in self.routing_buffer:
                self.routing_buffer[type_] = pd.concat([self.routing_buffer[type_], df])
            else:
                self.routing_buffer[type_] = df

        if self.manager.callbacks.empty:
            logger.debug('No subscribers/callbacks for metrics yet... skipped metrics')
            time.sleep(1)
            return

        # (for each metric type)
        # left join buffer and callbacks, group data by 'callback' then call callback w/ resulting dataframe
        for type_ in self.routing_buffer:
            try:
                for callback, incoming_chunks in pd.merge(
                        self.routing_buffer[type_], self.manager.callbacks,
                        how='left',
                        left_index=True,
                        right_index=True
                ).groupby('callback'):
                    # exec_time_start = time.time()
                    callback(incoming_chunks)
                    # logger.debug('Callback call took %.2f ms', (time.time() - exec_time_start) * 1000)
            except TypeError:
                logger.error('Trash/malformed data sinked into metric type `%s`. Data:\n%s',
                             type_, self.routing_buffer[type_], exc_info=True)

    def wait(self, timeout=None):
        self._finished.wait(timeout=timeout)

    def close(self):
        self._interrupted.set()
