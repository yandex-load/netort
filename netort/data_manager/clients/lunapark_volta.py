import logging
import requests
import threading
import time
import datetime
import os
import pkg_resources
import queue

from retrying import retry, RetryError

from ..common.interfaces import AbstractClient
from ..common.util import pretty_print

from netort.data_processing import get_nowait_from_queue

from requests.packages.urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)


logger = logging.getLogger(__name__)

RETRY_ARGS = dict(
    wrap_exception=True,
    stop_max_delay=10000,
    wait_fixed=1000,
    stop_max_attempt_number=5
)


@retry(**RETRY_ARGS)
def send_chunk(session, req, timeout=5):
    try:
        r = session.send(req, verify=False, timeout=timeout)
    except requests.ConnectionError:
        logger.warning('Connection error: %s', exc_info=True)
        raise
    except requests.HTTPError:
        logger.warning('Http error: %s', exc_info=True)
        raise
    else:
        logger.debug('Request %s code %s. Text: %s', r.url, r.status_code, r.text)
        r.raise_for_status()
        return r


class LunaparkVoltaClient(AbstractClient):
    create_job_url = '/mobile/create_job.json'
    update_job_url = '/mobile/update_job.json'
    metric_upload = '/api/volta/?query='
    dbname = 'volta'
    data_types_to_tables = {
        'current': 'currents',
        'syncs': 'syncs',
        'events': 'events',
        'metrics': 'metrics',
        'fragments': 'fragments',
        'unknown': 'logentries'
    }
    clickhouse_output_fmt = {
        'current': ['ts', 'value'],
        'syncs': ['ts', 'log_uts', 'app', 'tag', 'message'],
        'events': ['ts', 'log_uts', 'app', 'tag', 'value'],
        'metrics': ['ts', 'log_uts', 'app', 'tag', 'value'],
        'fragments': ['ts', 'log_uts', 'app', 'tag', 'message'],
        'unknown': ['ts', 'value']
    }

    def __init__(self, meta, job):
        super(LunaparkVoltaClient, self).__init__(meta, job)
        if self.meta.get('api_address'):
            self.api_address = self.meta.get('api_address')
        else:
            raise RuntimeError('Api address SHOULD be specified')
        self.clickhouse_cols = ['key_date', 'test_id']
        self.task = self.meta.get('task', 'LOAD-272')
        self.session = requests.session()
        self.key_date = "{key_date}".format(key_date=datetime.datetime.now().strftime("%Y-%m-%d"))
        self._job_number = None
        self.worker = WorkerThread(self)
        self.worker.start()
        logger.info('Lunapark Volta public job id: %s', self.job_number)

    @property
    def job_number(self):
        if not self._job_number:
            self._job_number = self.create_job()
            self.__test_id_link_to_jobno()
            return self._job_number
        else:
            return self._job_number

    def __test_id_link_to_jobno(self):
        """  create symlink local_id <-> public_id  """
        link_dir = os.path.join(self.job.artifacts_base_dir, 'lunapark_volta')
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
            logger.warning('Unable to create symlink for test: %s', self.job.job_id)
        else:
            logger.debug('Symlink created for job: %s', self.job.job_id)

    def put(self, df):
        self.pending_queue.put(df)

    def create_job(self):
        my_user_agent = None
        try:
            my_user_agent = pkg_resources.require('netort')[0].version
        except pkg_resources.DistributionNotFound:
            my_user_agent = 'DistributionNotFound'
        finally:
            headers = {
                #"Content-Type": "application/json",
                "User-Agent": "Uploader/{uploader_ua}, {upward_ua}".format(
                    upward_ua=self.meta.get('user_agent', ''),
                    uploader_ua=my_user_agent
                )
            }
        req = requests.Request(
            'POST',
            "{api_address}{path}".format(
                api_address=self.api_address,
                path=self.create_job_url
            ),
            headers=headers
        )
        req.data = {
            'key_date': self.key_date,
            'test_id': "{key_date}_{local_job_id}".format(
                key_date=self.key_date,
                local_job_id=self.job.job_id
            ),
            'task': self.task,
            'version': "2"
        }
        prepared_req = req.prepare()
        logger.debug('Prepared lunapark_volta create_job request:\n%s', pretty_print(prepared_req))

        try:
            response = send_chunk(self.session, prepared_req)
        except RetryError:
            logger.warning('Failed to create volta lunapark job', exc_info=True)
            raise
        else:
            logger.debug('Lunapark volta create job status: %s', response.status_code)
            logger.debug('Answ data: %s', response.json())
            job_id = response.json().get('jobno')
            if not job_id:
                logger.warning('Create job answ data: %s', response.json())
                raise ValueError('Lunapark volta returned answer without jobno: %s', response.json())
            else:
                logger.info('Lunapark volta job created: %s', job_id)
                return job_id

    def get_info(self):
        """ mock """
        pass

    def close(self):
        self.worker.stop()
        while not self.worker.is_finished():
            logger.debug('Processing pending uploader queue... qsize: %s', self.pending_queue.qsize())
        logger.debug('Joining uploader thread...')
        self.worker.join()
        logger.info('Uploader finished!')


class WorkerThread(threading.Thread):
    """ Process data """
    def __init__(self, client):
        super(WorkerThread, self).__init__()
        self._finished = threading.Event()
        self._interrupted = threading.Event()
        self.client = client
        self.session = requests.session()

    def run(self):
        while not self._interrupted.is_set():
            self.__process_pending_queue()
        logger.info('Lunapark volta uploader thread main loop interrupted, '
                    'finishing work and trying to send the rest of data, qsize: %s',
                    self.client.pending_queue.qsize())
        self.__process_pending_queue()
        self._finished.set()

    def __process_pending_queue(self):
        try:
            df = self.client.pending_queue.get_nowait()
        except queue.Empty:
            time.sleep(0.5)
        else:
            if df is not None:
                for metric_local_id, df_grouped_by_id in df.groupby(level=0):
                    df_grouped_by_id['key_date'] = self.client.key_date
                    df_grouped_by_id['test_id'] = "{key_date}_{local_job_id}".format(
                        key_date=self.client.key_date,
                        local_job_id=self.client.job.job_id
                    )
                    metric = self.client.job.manager.get_metric_by_id(metric_local_id)
                    if metric.type == 'events':
                        try:
                            metric_type = df_grouped_by_id['custom_metric_type'][0]
                        except KeyError:
                            metric_type = 'unknown'
                    else:
                        metric_type = 'current'
                    # logger.info('Df: %s', df_grouped_by_id)
                    body = df_grouped_by_id.to_csv(
                        sep='\t',
                        header=False,
                        index=False,
                        na_rep="",
                        columns=self.client.clickhouse_cols + self.client.clickhouse_output_fmt[metric_type]
                    )

                    req = requests.Request(
                        'POST', "{api}{data_upload_handler}{query}".format(
                            api=self.client.api_address,
                            data_upload_handler=self.client.metric_upload,
                            query="INSERT INTO {table} FORMAT TSV".format(
                                table="{db}.{type}".format(
                                    db=self.client.dbname,
                                    type=self.client.data_types_to_tables[metric_type]
                                )
                            )
                        )
                    )
                    req.data = body
                    prepared_req = req.prepare()
                    if metric.type == 'events':
                        logger.debug('Prepared upload request:\n%s', pretty_print(prepared_req))
                    try:
                        send_chunk(self.session, prepared_req)
                    except RetryError:
                        logger.warning(
                            'Failed to upload data to luna backend after consecutive retries.\n'
                            'Dropped data: \n%s', df_grouped_by_id, exc_info=True
                        )
                        return

    def is_finished(self):
        return self._finished

    def stop(self):
        self._interrupted.set()
        # FIXME
        logger.info('Lunapark Volta public job id: http://lunapark.yandex-team.ru/mobile/%s', self.client.job_number)
