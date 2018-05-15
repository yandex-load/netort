import logging
import requests
import queue as q
import threading
import datetime
import os

from retrying import retry, RetryError
from urlparse import urlparse

from ...data_processing import get_nowait_from_queue
from ..common.interfaces import AbstractClient

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
def send_chunk(url, data, timeout=5):
    r = requests.post(url, data=data, verify=False, timeout=timeout)
    r.raise_for_status()
    return r


class VoltaUploader(AbstractClient):
    """ Uploads data to Clickhouse
    have non-interface private method __upload_meta() for meta information upload
    """
    JOBNO_FNAME = 'jobno.log'
    dbname = 'volta'

    def __init__(self, meta, job):
        super(VoltaUploader, self).__init__(meta, job)
        self.addr = meta.get('address')
        self.hostname = urlparse(self.addr).scheme+'://'+urlparse(self.addr).netloc
        self.task = meta.get('task')
        self.test_id = meta.get('test_id')
        self.key_date = meta.get('key_date', "{key_date}".format(key_date=datetime.datetime.now().strftime("%Y-%m-%d")))
        self.create_job_url = meta.get('create_job_url')
        self.update_job_url = meta.get('update_job_url')
        self.data_types_to_tables = {
            'currents': 'currents',
            'sync': 'syncs',
            'event': 'events',
            'metric': 'metrics',
            'fragment': 'fragments',
            'unknown': 'logentries'
        }
        self.clickhouse_output_fmt = {
            'currents': ['key_date', 'test_id', 'uts', 'value'],
            'sync': ['key_date', 'test_id', 'sys_uts', 'log_uts', 'app', 'tag', 'message'],
            'event': ['key_date', 'test_id', 'sys_uts', 'log_uts', 'app', 'tag', 'message'],
            'metric': ['key_date', 'test_id', 'sys_uts', 'log_uts', 'app', 'tag', 'value'],
            'fragment': ['key_date', 'test_id', 'sys_uts', 'log_uts', 'app', 'tag', 'message'],
            'unknown': ['key_date', 'test_id', 'sys_uts', 'message']
        }
        self.operator = meta.get('operator')
        self._job_number = None
        self.inner_queue = q.Queue()
        self.worker = WorkerThread(self)
        self.worker.start()

    def put(self, data, type_):
        self.inner_queue.put((data, type_))

    @property
    def job_number(self):
        if not self._job_number:
            self._job_number = self.create_job()
            self.__test_id_link_to_jobno()
        return self._job_number

    def __test_id_link_to_jobno(self):
        link_dir = os.path.join(self.job.artifacts_base_dir, 'volta')
        if not os.path.exists(link_dir):
            os.makedirs(link_dir)
        try:
            os.symlink(
                os.path.join(
                    os.path.relpath(self.job.artifacts_base_dir, link_dir), self.test_id
                ),
                os.path.join(link_dir, str(self.job_number))
            )
        except OSError:
            logger.warning('Unable to create symlink for test: %s', self.test_id)
        else:
            logger.debug('Symlink created for job: %s', self.test_id)

    def create_job(self):
        data = {
            'key_date' : self.key_date,
            'test_id': self.test_id,
            'version': self.meta.get('version', 2),
            'task': self.task,
            'person': self.operator,
            'component': self.meta.get('component', None)
        }
        url = "{url}{path}".format(url=self.hostname, path=self.create_job_url)
        try:
            req = send_chunk(url, data)
        except RetryError:
            logger.warning('Failed to create Lunapark job')
            logger.debug('Failed to create Lunapark job', exc_info=True)
            raise RuntimeError('Failed to create Lunapark job')
        else:
            logger.debug('Lunapark create job status: %s', req.status_code)
            logger.debug('Req data: %s\nAnsw data: %s', data, req.json())

            if not req.json().get('success'):
                raise RuntimeError('Lunapark id not created: %s' % req.json()['error'])
            else:
                logger.info('Lunapark test id: %s', req.json()['jobno'])
                logger.info('Report url: %s/mobile/%s', self.hostname, req.json()['jobno'])
                return req.json()['jobno']

    def dump_jobno_to_file(self):
        try:
            with open(self.JOBNO_FNAME, 'w') as jobnofile:
                jobnofile.write(
                    "{path}/mobile/{jobno}".format(path=self.hostname, jobno=self.job_number)
                )
        except IOError:
            logger.error('Failed to dump jobno to file: %s', self.JOBNO_FNAME, exc_info=True)

    def update_job(self, data):
        url = "{url}{path}".format(url=self.hostname, path=self.update_job_url)
        try:
            req = send_chunk(url, data)
        except RetryError:
            logger.warning('Failed to update job metadata')
            logger.debug('Failed to update job metadata', exc_info=True)
        else:
            logger.debug('Lunapark update job status: %s', req.status_code)
            logger.debug('Req data: %s\nAnsw data: %s', data, req.json())

    def get_info(self):
        """ mock """
        pass

    def close(self):
        self.worker.stop()
        while not self.worker.is_finished():
            logger.debug('Processing pending uploader queue... qsize: %s', self.inner_queue.qsize())
        logger.debug('Joining uploader thread...')
        self.worker.join()
        logger.info('Uploader finished!')


class WorkerThread(threading.Thread):
    """ Process data

    read data from queue (each chunk is a tuple of (data,type)), send contents to clickhouse via http
        - data (pandas.DataFrame): dfs w/ data contents,
            differs for each data type.
            Should be processed differently from each other
        - type (string): dataframe type
    """
    def __init__(self, uploader):
        super(WorkerThread, self).__init__()
        self._finished = threading.Event()
        self._interrupted = threading.Event()
        self.uploader = uploader

    def run(self):
        while not self._interrupted.is_set():
            self.__get_from_queue_prepare_and_send()
        logger.info('Uploader thread main loop interrupted, '
                    'finishing work and trying to send the rest of data, qsize: %s',
                    self.uploader.inner_queue.qsize())
        self.__get_from_queue_prepare_and_send()
        self._finished.set()

    def __get_from_queue_prepare_and_send(self):
        pending_batch = self.__prepare_batch_of_chunks(
            get_nowait_from_queue(self.uploader.inner_queue)
        )
        for type_ in self.uploader.data_types_to_tables:
            if pending_batch[type_]:
                prepared_body = "".join(key for key in pending_batch[type_])
                url = "{addr}/?query={query}".format(
                    addr=self.uploader.addr,
                    query="INSERT INTO {db}.{type} FORMAT TSV".format(
                        db=self.uploader.dbname,
                        type=self.uploader.data_types_to_tables[type_]
                    )
                )
                try:
                    send_chunk(url, prepared_body)
                except RetryError:
                    logger.warning('Failed to send chunk via uploader. Dropped')
                    logger.debug('Failed to send chunk via uploader. Dropped: %s %s', url, prepared_body)

    def __prepare_batch_of_chunks(self, q_data):
        pending_data = {}
        for type_ in self.uploader.data_types_to_tables:
            pending_data[type_] = []
        for data, type_ in q_data:
            if data.empty:
                continue
            try:
                if type_ in self.uploader.data_types_to_tables:
                    data.loc[:, ('key_date')] = self.uploader.key_date
                    data.loc[:, ('test_id')] = self.uploader.test_id
                    data = data.to_csv(
                        sep='\t',
                        header=False,
                        index=False,
                        na_rep="",
                        columns=self.uploader.clickhouse_output_fmt.get(type_, [])
                    )
                    pending_data[type_].append(data)
                else:
                    logger.warning('Unknown data type for DataUplaoder, dropped: %s', exc_info=True)
            except Exception:
                logger.warning('Failed to format data for uploader of type %s.', type_)
                logger.debug('Failed to format data of type %s.\n data: %s', type_, data, exc_info=True)
        return pending_data

    def is_finished(self):
        return self._finished

    def stop(self):
        logger.info('Uploader got interrupt signal')
        self._interrupted.set()
