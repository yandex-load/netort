import queue as q
import threading
import time


def get_nowait_from_queue(queue):
    data = []
    for _ in range(queue.qsize()):
        try:
            data.append(queue.get_nowait())
        except q.Empty:
            break
    return data


class Drain(threading.Thread):
    """
    Drain a generator to a destination that answers to put(), in a thread
    """

    def __init__(self, source, destination):
        super(Drain, self).__init__()
        self.source = source
        self.destination = destination
        self._finished = threading.Event()
        self._interrupted = threading.Event()
        self.setDaemon(True)  # bdk+ytank stuck w/o this at join of this thread

    def run(self):
        for item in self.source:
            self.destination.put(item)
            if self._interrupted.is_set():
                break
        self._finished.set()

    def wait(self, timeout=None):
        self._finished.wait(timeout=timeout)

    def close(self):
        self._interrupted.set()


class Tee(threading.Thread):
    """
    Drain a queue and put its contents to list of destinations
    """

    def __init__(self, source, destination, type):
        super(Tee, self).__init__()
        self.source = source
        self.destination = destination
        self.type = type
        self._finished = threading.Event()
        self._interrupted = threading.Event()
        self.setDaemon(True)  # just in case, bdk+ytank stuck w/o this at join of Drain thread

    def run(self):
        while not self._interrupted.is_set():
            data = get_nowait_from_queue(self.source)
            for item in data:
                for destination in self.destination:
                    destination.put(item, self.type)
                    if self._interrupted.is_set():
                        break
                if self._interrupted.is_set():
                    break
            if self._interrupted.is_set():
                break
            time.sleep(0.5)
        self._finished.set()

    def wait(self, timeout=None):
        self._finished.wait(timeout=timeout)

    def close(self):
        self._interrupted.set()


class Chopper(object):
    def __init__(self, source):
        self.source = source

    def __iter__(self):
        for chunk in self.source:
            for item in chunk:
                yield item
