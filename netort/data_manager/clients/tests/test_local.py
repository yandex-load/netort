import os
import json
import pathlib
import numpy as np
import pandas as pd
from time import time
from netort.data_manager import DataSession

import logging
logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.INFO)

import pytest


@pytest.fixture()
def empty_data_frame():
    return pd.DataFrame(columns=['ts', 'value'])


@pytest.fixture()
def trivial_data_frame():
    return pd.DataFrame([[0, 0]], columns=['ts', 'value'])

def test_dir_created(tmp_path):
    artifacts_base_dir = tmp_path / "logs"
    config = {
        'clients': [
            {
                'type': 'local_storage',
            }
        ],
        'test_start': int(time()*1e6),
        'artifacts_base_dir': str(artifacts_base_dir)
    }
    data_session = DataSession(config=config)
    # TODO: make this pass. Datasession dir and meta.json should be created as soon as possible
    # assert os.path.isdir(artifacts_base_dir), "Artifacts base dir should exist after datasession have been created"
    # assert os.path.isdir(data_session.artifacts_dir), "Artifacts dir should exist after datasession have been created"
    data_session.close()
    assert os.path.isdir(artifacts_base_dir), "Artifacts base dir should exist after datasession have ended"
    assert os.path.isdir(data_session.artifacts_dir), "Artifacts dir should exist after datasession have ended"
    assert os.path.isfile(pathlib.Path(data_session.artifacts_dir) / 'meta.json'), "Metadata file should have been created"


    with open(pathlib.Path(data_session.artifacts_dir) / 'meta.json') as meta_file:
        meta = json.load(meta_file)
    
    assert 'job_meta' in meta, "Metadata should have been written to meta.json"


def test_metric_created(tmp_path, trivial_data_frame):
    artifacts_base_dir = tmp_path / "logs"
    config = {
        'clients': [
            {
                'type': 'local_storage',
            }
        ],
        'test_start': int(time()*1e6),
        'artifacts_base_dir': str(artifacts_base_dir)
    }
    data_session = DataSession(config=config)
    metric = data_session.new_true_metric(
        "My Raw Metric",
        raw=True, aggregate=False,
        hostname='localhost',
        source='PyTest',
        group='None'
    )
    metric.put(trivial_data_frame)
    # TODO: make this pass. Metric should be created as soon as possible after it was created
    # assert os.path.isdir(metric_path), "Artifacts base dir should exist after datasession have been created"
    data_session.close()
    with open(pathlib.Path(data_session.artifacts_dir) / 'meta.json') as meta_file:
        meta = json.load(meta_file)
    print(meta)

    assert 'metrics' in meta, "Metrics should have been written to meta.json"
    assert len(meta['metrics']) == 1, "Exactly one metric should have been written to meta.json"

    metric_id = list(meta['metrics'])[0]
    assert os.path.isfile(pathlib.Path(data_session.artifacts_dir) / f'{metric_id}.data'), "Metric data should have been written"


# def test_meta_data(tmp_path):
#     artifacts_base_dir = tmp_path #/ "logs"
#     config = {
#         'clients': [
#             {
#                 'type': 'local_storage',
#             }
#         ],
#         'test_start': int(time()*1e6),
#         'artifacts_base_dir': artifacts_base_dir
#     }
#     data_session = DataSession(config=config)
#     data_session.close()

# def test_raw_metric(data_session):
#     SIZE = 100
#     metric_obj = data_session.new_true_metric(
#         "My Raw Metric",
#         raw=True, aggregate=False,
#         hostname='localhost',
#         source='Jupyter',
#         group='None'
#     )

#     X = (np.arange(SIZE) * 1e4).astype(int)

#     df = pd.DataFrame()
#     df['ts'] = X
#     Xdot = X * 1e-6
#     df['value'] = np.sin(Xdot)
#     metric_obj.put(df)

# # Агрегированная метрика
# metric_obj = data_session.new_true_metric(
#     "My Aggregated Metric",
#     raw=False, aggregate=True,
#     hostname='localhost',
#     source='Jupyter',
#     group='None'
# )

# df = pd.DataFrame()
# df['ts'] = X
# df['value'] = np.sin(X * 1e-7) + np.random.normal(size=SIZE)
# metric_obj.put(df)

# # агрегированные эвенты
# metric_obj = data_session.new_event_metric(
#     "My Aggregated Events",
#     raw=False, aggregate=True,
#     hostname='localhost',
#     source='Jupyter',
#     group='None'
# )

# df = pd.DataFrame()
# df['ts'] = X
# df['value'] = np.random.choice("a quick brown fox jumped over the lazy dog".split(), len(X))
# metric_obj.put(df)

# # неагрегированные эвенты (тут Луна пока что валится)
# # metric_obj = data_session.new_event_metric(
# #     "My Aggregated Events",
# #     raw=True, aggregate=False,
# #     hostname='localhost',
# #     source='Jupyter',
# #     group='None'
# # )

# # df = pd.DataFrame()
# # df['ts'] = X
# # df['value'] = np.random.choice("a quick brown fox jumped over the lazy dog".split(), len(X))
# # metric_obj.put(df)
