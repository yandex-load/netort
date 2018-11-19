from ..common.interfaces import AbstractClient

import io
import os
import logging
import json
import threading
import queue
import time

logger = logging.getLogger(__name__)


""" 
output artifact sample:

{"dtypes": {"ts": "int64", "value": "float64"}, "type": "metric", "names": ["ts", "value"]}
123	123.123
456	456.456
123	123.123
456	456.456

output meta.json sample:
{
    "metrics": {
        "metric_d12dab4f-e4ef-4c47-89e6-859f73737c64": {
            "dtypes": {
                "ts": "int64",
                "value": "float64"
            },
            "meta": {
                "hostname": "localhost",
                "name": "cpu_usage",
                "some_meta_key": "some_meta_value",
                "type": "metrics"
            },
            "names": [
                "ts",
                "value"
            ],
            "type": "metrics"
        },
    },
    "job_meta": {
        "key": "valueZ",
    }
}
"""


class LocalStorageClient(AbstractClient):
    separator = '\t'
    metrics_meta_fname = 'meta.json'

    def __init__(self, meta, job):
        super(LocalStorageClient, self).__init__(meta, job)
        self.registered_meta = {}

        self.processing_thread = ProcessingThread(self)
        self.processing_thread.setDaemon(True)
        self.processing_thread.start()

    def put(self, df):
        if df is not None:
            self.pending_queue.put(df)

    def close(self):
        self.processing_thread.stop()
        logger.info('Joining local client processing thread...')
        self.processing_thread.join()
        logger.info('Local client finished its work. Artifacts are here %s', self.job.artifacts_dir)


class ProcessingThread(threading.Thread):
    """ Process data """
    def __init__(self, client):
        super(ProcessingThread, self).__init__()
        self._finished = threading.Event()
        self._interrupted = threading.Event()
        self.client = client
        self.file_streams = {}

    def __create_artifact(self, metric):
        self.file_streams[metric.local_id] = io.open(
            os.path.join(
                self.client.job.artifacts_dir, "{id}.data".format(id=metric.local_id)
            ),
            mode='wb'
        )

    def run(self):
        while not self._interrupted.is_set():
            self.__process_pending_queue()
        logger.info(
            'File writer thread interrupted, finishing work and trying to write the rest of data, qsize: %s',
            self.client.pending_queue.qsize()
        )
        self.__process_pending_queue()
        self.__close_files_and_dump_meta()
        self._finished.set()

    def __process_pending_queue(self):
        exec_time_start = time.time()
        try:
            incoming_df = self.client.pending_queue.get_nowait()
            df = incoming_df.copy()
        except queue.Empty:
            time.sleep(1)
        else:
            for metric_local_id, df_grouped_by_id in df.groupby(level=0):
                metric = self.client.job.manager.get_metric_by_id(metric_local_id)
                if not metric:
                    logger.warning('Received unknown metric id: %s', metric_local_id)
                    return
                if metric.local_id not in self.file_streams:
                    logger.debug('Creating artifact file for %s', metric.local_id)
                    self.__create_artifact(metric)
                    dtypes = {}
                    for name, type_ in metric.dtypes.items():
                        dtypes[name] = type_.__name__
                    this_metric_meta = {
                        'type': metric.type,
                        'names': metric.columns,
                        'dtypes': dtypes,
                        'meta': metric.meta
                    }
                    self.client.registered_meta[metric.local_id] = this_metric_meta
                    artifact_file_header = json.dumps(this_metric_meta)
                    self.file_streams[metric.local_id].write("%s\n" % artifact_file_header)
                csv_data = df_grouped_by_id.to_csv(
                    sep=self.client.separator,
                    header=False,
                    index=False,
                    na_rep="",
                    columns=metric.columns
                )
                logger.debug('Local storage client after to csv method took %.2f ms',
                             (time.time() - exec_time_start) * 1000)
                try:
                    self.file_streams[metric.local_id].write(
                        csv_data
                    )
                    self.file_streams[metric.local_id].flush()
                except ValueError:
                    logger.warning('Failed to write metrics to file, maybe file is already closed?', exc_info=True)
        logger.debug('Local storage client processing took %.2f ms', (time.time() - exec_time_start) * 1000)

    def is_finished(self):
        return self._finished

    def __close_files_and_dump_meta(self):
        [self.file_streams[file_].close() for file_ in self.file_streams]
        with open(os.path.join(self.client.job.artifacts_dir, self.client.metrics_meta_fname), 'wb') as meta_f:
            json.dump(
                {"metrics": self.client.registered_meta, "job_meta": self.client.meta},
                meta_f,
                indent=4,
                sort_keys=True
            )

    def stop(self):
        self._interrupted.set()
