import threading
import time
import pandas as pd
import logging

from netort.data_manager.metrics import Aggregate
from netort.data_manager.metrics.aggregate import Aggregator

from ..data_processing import get_nowait_from_queue

logger = logging.getLogger(__name__)


class MetricsRouter(threading.Thread):
    """
    Drain incoming queue, concatenate dataframes by metric type and process to callbacks
    callback receives resulting dataframe
    """

    # TODO: MetricsRouter should not know anything about DataManager. Pass source and subscribers directly.
    def __init__(self, manager, aggregator_buffer_size=10):
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
        while self.manager.routing_queue.qsize() > 1:
            self.__route()
        self.__route(last_piece=True)
        logger.info('Router finished its work')
        self._finished.set()

    def __from_aggregator_buffer(self, df, metric_id, last_piece):
        # type: (pd.DataFrame) -> pd.DataFrame
        """

        :rtype: pd.DataFrame
        """
        buf = self.__aggregator_buffer.get(metric_id)
        df['second'] = (df['ts'] / 1000000).astype(int)

        if buf is not None:
            df = pd.concat([buf, df])

        if not last_piece:
            cut, new_buf = df[df.second < df.second.max() - self.aggregator_buffer_size], \
                           df[df.second >= df.second.max() - self.aggregator_buffer_size]
            self.__aggregator_buffer[metric_id] = new_buf
            return cut
        else:
            self.__aggregator_buffer[metric_id] = None
            return df

    def __route(self, last_piece=False):
        routing_buffer = {}
        all_data = get_nowait_from_queue(self.manager.routing_queue)
        for entry in all_data:
            if entry.type == Aggregate.type:
                cut = self.__from_aggregator_buffer(entry.df, entry.local_id, last_piece) #.groupby('second')
                if cut.empty:
                    continue
                else:
                    data = Aggregator.aggregate(cut)
                    data['metric_local_id'] = entry.local_id
                    data = data.set_index('metric_local_id')
            else:
                data = entry.df

            if entry.type in routing_buffer:
                routing_buffer[entry.type] = pd.concat([routing_buffer[entry.type], data], sort=False)
            else:
                routing_buffer[entry.type] = data
        if last_piece:
            for metric_id, df in self.__aggregator_buffer.items():
                data = Aggregator.aggregate(df)
                data['metric_local_id'] = metric_id
                data = data.set_index('metric_local_id')
                if Aggregate.type in routing_buffer:
                    routing_buffer[Aggregate.type] = pd.concat([routing_buffer[Aggregate.type], data], sort=False)
                else:
                    routing_buffer[Aggregate.type] = data

        if self.manager.callbacks.empty:
            logger.debug('No subscribers/callbacks for metrics yet... skipped metrics')
            time.sleep(1)
            return

        # (for each metric type)
        # left join buffer and callbacks, group data by 'callback' then call callback w/ resulting dataframe
        for type_ in routing_buffer:
            try:
                router = pd.merge(
                    routing_buffer[type_], self.manager.callbacks,
                    how='left',
                    left_index=True,
                    right_index=True
                ).groupby('callback', sort=False)
                for callback, incoming_chunks in router:
                    # exec_time_start = time.time()
                    callback(incoming_chunks)
                    logger.debug('Callback to {}'.format(callback))
                    # logger.debug('Callback call took %.2f ms', (time.time() - exec_time_start) * 1000)
            except TypeError:
                logger.error('Trash/malformed data sinked into metric type `%s`. Data:\n%s',
                             type_, routing_buffer[type_], exc_info=True)

    def wait(self, timeout=None):
        self._finished.wait(timeout=timeout)

    def close(self):
        self._interrupted.set()
