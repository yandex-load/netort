from requests import HTTPError, ConnectionError
from requests.exceptions import Timeout, TooManyRedirects

from ..common.interfaces import AbstractClient, QueueWorker
from ..common.util import pretty_print

from retrying import retry

import pkg_resources
import logging
import requests
import threading
import time
import six
import pandas as pd
import datetime
import os
import six
if six.PY2:
    import queue
else:
    import Queue as queue

requests.packages.urllib3.disable_warnings()

logger = logging.getLogger(__name__)


RETRY_ARGS = dict(
    stop_max_delay=30000,
    wait_fixed=3000,
    stop_max_attempt_number=10
)

SLEEP_ON_EMPTY = 0.2  # pause in seconds before checking empty queue on new items
MAX_DF_LENGTH = 20000  # Size of chunk is 20k rows, it's approximately 1Mb in csv


@retry(**RETRY_ARGS)
def send_chunk(session, req, timeout=5):
    r = session.send(req, verify=False, timeout=timeout)
    logger.debug('Request %s code %s. Text: %s', r.url, r.status_code, r.text)
    return r


def if_not_failed(func):
    def wrapped(self, *a, **kw):
        if self.failed.is_set():
            logger.warning('Luna client is disabled')
            return
        else:
            return func(self, *a, **kw)
    return wrapped


class LunaClient(AbstractClient):
    create_metric_path = '/create_metric/'
    update_metric_path = '/update_metric/'
    upload_metric_path = '/upload_metric/?query='  # production
    create_job_path = '/create_job/'
    update_job_path = '/update_job/'
    close_job_path = '/close_job/'
    symlink_artifacts_path = 'luna'

    def __init__(self, meta, job):
        super(LunaClient, self).__init__(meta, job)
        logger.debug('Luna client local id: %s', self.local_id)
        self.dbname = meta.get('db_name', 'luna')
        self.failed = threading.Event()
        self.public_ids = {}
        self.luna_columns = ['key_date', 'tag']
        self.key_date = "{key_date}".format(key_date=datetime.datetime.now().strftime("%Y-%m-%d"))
        self._interrupted = threading.Event()
        self.register_worker = RegisterWorkerThread(self)
        self.register_worker.start()
        self.worker = WorkerThread(self)
        self.worker.start()
        self.session = requests.session()

        if self.meta.get('api_address'):
            self.api_address = self.meta.get('api_address')
        else:
            raise RuntimeError('Api address SHOULD be specified')
        self._job_number = None

    @property
    def job_number(self):
        if self.failed.is_set():
            return
        # FIXME: job_number should be a property
        if not self._job_number:
            try:
                self._job_number = self.create_job()
                self.__test_id_link_to_jobno()
            except (HTTPError, ConnectionError, Timeout, TooManyRedirects):
                logger.error('Failed to create Luna job', exc_info=True)
                self.failed.set()
                self.interrupt()
        return self._job_number

    def put(self, data_type, df):
        if not self.failed.is_set():
            self.pending_queue.put((data_type, df))
        else:
            logger.debug('Skipped incoming data chunk due to failures')

    @if_not_failed
    def create_job(self):
        """ Create public Luna job

        Returns:
            job_id (basestring): Luna job id
        """
        my_user_agent = None
        try:
            my_user_agent = pkg_resources.require('netort')[0].version
        except pkg_resources.DistributionNotFound:
            my_user_agent = 'DistributionNotFound'
        finally:
            headers = {
                "User-Agent": "Uploader/{uploader_ua}, {upward_ua}".format(
                    upward_ua=self.meta.get('user_agent', ''),
                    uploader_ua=my_user_agent
                )
            }
        req = requests.Request(
            'POST',
            "{api_address}{path}".format(
                api_address=self.api_address,
                path=self.create_job_path
            ),
            headers=headers
        )
        req.data = {
            'test_start': self.job.test_start
        }
        prepared_req = req.prepare()
        logger.debug('Prepared create_job request:\n%s', pretty_print(prepared_req))

        response = send_chunk(self.session, prepared_req)
        logger.debug('Luna create job status: %s', response.status_code)
        response.raise_for_status()
        logger.debug('Answ data: %s', response.content)
        job_id = response.content.decode('utf-8') if isinstance(response.content, bytes) else response.content
        if not job_id:
            self.failed.set()
            raise ValueError('Luna returned answer without jobid: %s', response.content)
        else:
            logger.info('Luna job created: %s', job_id)
            return job_id

    @if_not_failed
    def update_job(self, meta):
        req = requests.Request(
            'POST',
            "{api_address}{path}?job={job}".format(
                api_address=self.api_address,
                path=self.update_job_path,
                job=self.job_number
            ),
        )
        req.data = meta
        prepared_req = req.prepare()
        logger.debug('Prepared update_job request:\n%s', pretty_print(prepared_req))
        response = send_chunk(self.session, prepared_req)
        logger.debug('Update job status: %s', response.status_code)
        logger.debug('Answ data: %s', response.content)

    @if_not_failed
    def update_metric(self, meta):
        for metric_tag, metric_obj in self.job.manager.metrics.items():
            if not metric_obj.tag:
                logger.debug('Metric %s has no public tag, skipped updating metric', metric_tag)
                continue
            req = requests.Request(
                'POST',
                "{api_address}{path}?tag={tag}".format(
                    api_address=self.api_address,
                    path=self.update_metric_path,
                    tag=metric_obj.tag
                ),
            )
            req.data = meta
            # FIXME: should be called '_offset' after volta-service production is updated;
            if 'sys_uts_offset' in meta and metric_obj.type == 'metrics':
                req.data['offset'] = meta['sys_uts_offset']
            elif 'log_uts_offset' in meta and metric_obj.type == 'events':
                req.data['offset'] = meta['log_uts_offset']
            prepared_req = req.prepare()
            logger.debug('Prepared update_metric request:\n%s', pretty_print(prepared_req))
            response = send_chunk(self.session, prepared_req)
            logger.debug('Update metric status: %s', response.status_code)
            logger.debug('Answ data: %s', response.content)

    @if_not_failed
    def _close_job(self):
        req = requests.Request(
            'GET',
            "{api_address}{path}".format(
                api_address=self.api_address,
                path=self.close_job_path,
            ),
            params={'job': self._job_number}
        )
        prepared_req = req.prepare()
        logger.debug('Prepared close_job request:\n%s', pretty_print(prepared_req))
        response = send_chunk(self.session, prepared_req)
        logger.debug('Update job status: %s', response.status_code)

    def __test_id_link_to_jobno(self):
        """  create symlink local_id <-> public_id  """
        link_dir = os.path.join(self.job.artifacts_base_dir, self.symlink_artifacts_path)
        if not self._job_number:
            logger.info('Public test id not available, skipped symlink creation for %s', self.symlink_artifacts_path)
            return
        if not os.path.exists(link_dir):
            os.makedirs(link_dir)
        try:
            os.symlink(
                os.path.join(
                    os.path.relpath(self.job.artifacts_base_dir, link_dir), self.job.job_id
                ),
                os.path.join(link_dir, str(self.job_number))
            )
        except OSError:
            logger.warning(
                'Unable to create %s/%s symlink for test: %s',
                self.symlink_artifacts_path, self.job_number, self.job.job_id
            )
        else:
            logger.debug(
                'Symlink %s/%s created for job: %s',
                self.symlink_artifacts_path, self.job_number, self.job.job_id
            )

    def close(self):
        self.register_worker.stop()
        logger.info('Joining luna client metric registration thread...')
        self.register_worker.join()
        self.worker.stop()
        if not self.worker.is_finished():
            logger.debug('Processing pending uploader queue... qsize: %s', self.pending_queue.qsize())
        logger.info('Joining luna client metric uploader thread...')
        self.worker.join()
        self._close_job()
        # FIXME hardcoded host
        # FIXME we dont know front hostname, because api address now is clickhouse address
        logger.info('Luna job url: %s%s', 'https://volta.yandex-team.ru/tests/', self.job_number)
        logger.info('Luna client done its work')

    def interrupt(self):
        logger.warning('Luna client work was interrupted.')
        self.put = lambda *a, **kw: None
        self.register_worker.interrupt()
        self.worker.interrupt()


class RegisterWorkerThread(QueueWorker):
    """ Register metrics metadata, get public_id from luna and create map local_id <-> public_id """
    def __init__(self, client):
        """
        :type client: LunaClient
        """
        super(RegisterWorkerThread, self).__init__(queue.Queue())
        self.client = client
        self.session = requests.session()
        for callback, ids in self.client.job.manager.callbacks.groupby('callback', sort=False):
            if callback == self.client.put:
                for id_ in ids.index:
                    if id_ not in self.client.public_ids:
                        metric = self.client.job.manager.get_metric_by_id(id_)
                        self.queue.put(metric)

    def register(self, metric):
        self.queue.put(metric)

    def _process_pending_queue(self, progress=False):
        try:
            metric = self.queue.get_nowait()
            if metric.local_id in self.client.public_ids:
                return
            metric.tag = self._register_metric(metric)
            logger.debug(
                'Successfully received tag %s for metric.local_id: %s (%s)',
                metric.tag, metric.local_id, metric.meta)
            self.client.public_ids[metric.local_id] = metric.tag
        except (HTTPError, ConnectionError, Timeout, TooManyRedirects):
            logger.error("Luna service unavailable", exc_info=True)
            self.client.interrupt()
        except queue.Empty:
            time.sleep(0.5)

    def _register_metric(self, metric):
        json = {
            'job': self.client.job_number,
            'type': metric.type.table_name,
            'types': [t.table_name for t in metric.data_types],
            'local_id': metric.local_id,
            'meta': metric.meta
        }
        req = requests.Request(
            'POST',
            "{api_address}{path}".format(
                api_address=self.client.api_address,
                path=self.client.create_metric_path
            ),
            json=json
        )
        prepared_req = req.prepare()
        logger.debug('Prepared create_metric request:\n%s', pretty_print(prepared_req))
        response = send_chunk(self.session, prepared_req)
        response.raise_for_status()
        if not response.content:
            raise HTTPError('Luna did not return uniq_id for metric registration: %s', response.content)
        else:
            return response.content.decode('utf-8') if six.PY3 else response.content


# noinspection PyTypeChecker
class WorkerThread(QueueWorker):
    """ Process data """
    def __init__(self, client):
        super(WorkerThread, self).__init__(client.pending_queue)
        self.data = {'max_length': 0}
        self.client = client
        self.session = requests.session()

    def run(self):
        while not self._stopped.is_set():
            self._process_pending_queue()
        while self.queue.qsize() > 0 and not self._interrupted.is_set():
            self._process_pending_queue(progress=True)
        while self.queue.qsize() > 0:
            self.queue.get_nowait()
        if self.data['max_length'] != 0:
            self.__upload_data()
        self._finished.set()

    def _process_pending_queue(self, progress=False):

        try:
            data_type, raw_df = self.queue.get_nowait()
            if progress:
                logger.info("{} entries in queue remaining".format(self.client.pending_queue.qsize()))
        except queue.Empty:
            time.sleep(SLEEP_ON_EMPTY)
        else:
            self.__update_df(data_type, raw_df)

    def __update_max_length(self, new_length):
        return new_length if self.data['max_length'] < new_length else self.data['max_length']

    def __update_df(self, data_type, input_df):
        for metric_local_id, df_grouped_by_id in input_df.groupby(level=0, sort=False):
            metric = self.client.job.manager.get_metric_by_id(metric_local_id)

            if not metric:
                logger.warning('Received unknown metric: %s! Ignored.', metric_local_id)
                return pd.DataFrame([])

            if metric.local_id not in self.client.public_ids:
                # no public_id yet, put it back
                self.client.put(data_type, input_df)
                logger.debug('No public id for metric {}'.format(metric.local_id))
                self.client.register_worker.register(metric)
                return pd.DataFrame([])

            df_grouped_by_id.loc[:, 'key_date'] = self.client.key_date
            df_grouped_by_id.loc[:, 'tag'] = self.client.public_ids[metric.local_id]
            # logger.debug('Groupped by id:\n{}'.format(df_grouped_by_id.head()))
            # logger.debug('Metric: {} columns: {}'.format(metric, metric.columns))
            result_df = df_grouped_by_id

            if not result_df.empty:
                table_name = data_type.table_name
                if not self.data.get(table_name):
                    self.data[table_name] = {
                        'dataframe': result_df,
                        'columns': self.client.luna_columns + data_type.columns,
                    }
                else:
                    self.data[table_name]['dataframe'] = pd.concat([self.data[table_name]['dataframe'], result_df])
                self.data['max_length'] = self.__update_max_length(len(self.data[table_name]['dataframe']))

        if self.data['max_length'] >= MAX_DF_LENGTH:
            self.__upload_data()

    def __upload_data(self):
        for table_name, data in self.data.items():
            if table_name is not 'max_length':
                logger.debug('Length of data for %s is %s', table_name, len(data['dataframe']))
                try:
                    self.__send_upload(table_name, data['dataframe'], data['columns'])

                except ConnectionError:
                    logger.warning('Failed to upload data to luna backend after consecutive retries. '
                                   'Attempt to send data in two halves')
                    try:
                        self.__send_upload(
                            table_name,
                            data['dataframe'].head(len(data['dataframe']) // 2),
                            data['columns']
                        )
                        self.__send_upload(
                            table_name,
                            data['dataframe'].tail(len(data['dataframe']) - len(data['dataframe']) // 2),
                            data['columns']
                        )
                    except ConnectionError:
                        logger.warning('Failed to upload data to luna backend after consecutive retries. Sorry.')
                        return

                self.data = dict()
                self.data['max_length'] = 0

    def __send_upload(self, table_name, df, columns):
        body = df.to_csv(
            sep='\t',
            header=False,
            index=False,
            na_rep='',
            columns=columns
        )
        req = requests.Request(
            'POST', "{api}{data_upload_handler}{query}".format(
                api=self.client.api_address,  # production proxy
                data_upload_handler=self.client.upload_metric_path,
                query="INSERT INTO {table} FORMAT TSV".format(
                    table="{db}.{type}".format(db=self.client.dbname, type=table_name)  # production
                )
            )
        )
        req.headers = {
            'X-ClickHouse-User': 'lunapark',
            'X-ClickHouse-Key': 'lunapark'
        }
        req.data = body
        prepared_req = req.prepare()
        # logger.debug('Body content length: %s, df size: %s; content is\n%s',
        #              prepared_req.headers['Content-Length'], df.shape, df)

        try:
            resp = send_chunk(self.session, prepared_req)
            resp.raise_for_status()
            logger.info('Update table %s with %s rows -- successful', table_name, df.shape[0])

        except ConnectionError:
            raise
        except (HTTPError, Timeout, TooManyRedirects) as e:
            # noinspection PyUnboundLocalVariable
            logger.warning('Failed to upload data to luna. Dropped some data.\n{}'.
                           format(resp.content if isinstance(e, HTTPError) else 'no response'))
            logger.debug(
                'Failed to upload data to luna backend after consecutive retries.\n'
                'Dropped data head: \n%s', df.head(), exc_info=True
            )
            self.client.interrupt()

            logger.warning('Failed to upload data to luna. Dropped some data.\n{}'.format(resp.content))
