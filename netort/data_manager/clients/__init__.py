from .local import LocalStorageClient
from .luna import LunaClient
from .volta import VoltaUploader


available_clients = {
    'luna': LunaClient,
    'local_storage': LocalStorageClient
}
