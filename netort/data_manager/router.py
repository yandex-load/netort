import threading
import time
from Queue import Empty

import pandas as pd
import logging

from netort.data_manager.common.interfaces import Aggregated

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
        :type manager: netort.data_manager.DataManager
        """
        super(MetricsRouter, self).__init__()
        self.aggregator_buffer_size = aggregator_buffer_size
        self.manager = manager
        self._finished = threading.Event()
        self._stopped = threading.Event()
        self._interrupted = threading.Event()
        self.setDaemon(True)  # just in case, bdk+ytank stuck w/o this at join of Drain thread
        self.__buffer = {}

    def run(self):
        while not self._stopped.is_set():
            self.__route()
        logger.info('Router received interrupt signal, routing rest of the data. Qsize: %s',
                    self.manager.routing_queue.qsize())
        while self.manager.routing_queue.qsize() > 1 and not self._interrupted.is_set():
            self.__route()
        self.__route(last_piece=True)
        logger.info('Router finished its work')
        self._finished.set()

    def _from_buffer(self, metric_data, last_piece):
        """
        :type metric_data: netort.data_manager.common.interfaces.MetricData
        :rtype: pd.DataFrame
        """

        buffered = self.__buffer.pop(metric_data.local_id, None)
        df = pd.concat([buffered, metric_data.df]) if buffered is not None else metric_data.df
        if not last_piece:
            cut, new_buf = df[df.second < df.second.max() - Aggregated.buffer_size], \
                           df[df.second >= df.second.max() - Aggregated.buffer_size]
            self.__buffer[metric_data.local_id] = new_buf
            return cut
        else:
            return df

    def __route(self, last_piece=False):
        # all_data = get_nowait_from_queue(self.manager.routing_queue)
        try:
            metric_data = self.manager.routing_queue.get_nowait()
        except Empty:
            return
        routed_data = {}
        if metric_data.is_aggregated:
            from_buffer = self._from_buffer(metric_data, last_piece)
        for dtype in metric_data.data_types:
            unprocessed = from_buffer if dtype.is_aggregated() else metric_data.df
            if unprocessed.empty:
                continue
            t = time.time()
            processed = self.reindex_to_local_id(
                dtype.processor(unprocessed, last_piece),
                metric_data.local_id)
            logger.debug('Processing {} of length {} took {} seconds'.format(dtype.__name__,
                                                                             len(unprocessed),
                                                                             time.time()-t))
            if not processed.empty:
                routed_data.setdefault(dtype, []).append(
                    processed
                )
        if last_piece:
            for metric_local_id, df in self.__buffer.items():
                d_types = self.manager.metrics[metric_local_id].data_types
                for d_type in [dt for dt in d_types if dt.is_aggregated()]:
                    processed = self.reindex_to_local_id(
                        d_type.processor(df, last_piece),
                        metric_local_id
                    )
                    routed_data.setdefault(d_type, []).append(
                        processed
                    )
        routed_data = {
            dtype: pd.concat(dfs) for dtype, dfs in routed_data.items()
        }
        if self.manager.callbacks.empty:
            logger.debug('No subscribers/callbacks for metrics yet... skipped metrics')
            time.sleep(1)
            return
        # (for each metric type)
        # left join buffer and callbacks, group data by 'callback' then call callback w/ resulting dataframe
        for data_type in routed_data:
            try:
                router = pd.merge(
                    routed_data[data_type], self.manager.callbacks,
                    how='left',
                    left_index=True,
                    right_index=True
                ).groupby('callback', sort=False)
                for callback, incoming_chunks in router:
                    callback(data_type, incoming_chunks)
            except TypeError:
                logger.error('Trash/malformed data sinked into metric type `%s`. Data:\n%s',
                             data_type, routed_data[data_type], exc_info=True)

    @staticmethod
    def reindex_to_local_id(df, local_id):
        df['metric_local_id'] = local_id
        return df.set_index('metric_local_id')

    def wait(self, timeout=None):
        self._finished.wait(timeout=timeout)

    def close(self):
        self._stopped.set()

    def interrupt(self):
        self.close()
        logger.debug('Routing interrupted')
        self._interrupted.set()