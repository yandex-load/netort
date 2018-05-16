from .local import LocalStorageClient
from .luna import LunaClient


available_clients = {
    'luna': LunaClient,
    'local_storage': LocalStorageClient
}
