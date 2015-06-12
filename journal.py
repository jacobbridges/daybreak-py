__author__ = 'stanley'

import os
from .FileLock import FileLock
from Queue import Queue
from threading import Thread


class Journal(Queue):

    def __init__(self, file_path, storage_format, serializer, block):
        """
        :param filepath: Path to journal file
        :param storage_format: Format to save the db contents to the journal file
        :param serializer:
        :param block:
        :return:
        """

        # Call to super class's constructor
        Queue.__init__(self)

        self.file_path = file_path
        self.format = storage_format
        self.serializer = serializer
        self.emit = block

        self.file_lock = FileLock(file_path)
        self.open()

        self.worker = Thread(name='journal_worker', target=self.worker)
        self.load()
        pass

    def size(self):
        return self.qsize()

    def closed(self):
        return self.file.closed

    def close(self):
        with self.mutex:
            self.queue.clear()
        self.worker.join()
        self.file.close()

    def load(self):
        self.file.flush()
        self.join()
        self.replay()

    def lock(self):
        self.flush()
        with self.file_lock:
            self.replay()
            self.flush()

    def clear(self):
        self.flush()

    def flush(self):
        self.join()
        self.file.flush() if not self.closed else None

    def compact(self):
        pass

    def bytesize(self):
        pass

    def replay(self):
        buf = buffer(bytearray(self.file.readall()))
        self.size += self.format.parse(buf, self.emit)

    def open(self):
        self.file.close() if self.file is not None else None
        self.file = open(self.file_path, 'ab+')
        stat = os.stat(self.file_path)
        self.inode = stat.st_ino
        self.file.write(self.format.header) if stat.st_size == 0 else None
        self.pos = None

    def read(self):
        pass

    def dump(self):
        pass

    def worker(self):
        pass

    def writer(self, dump):
        pass

    def with_flock(self, mode):
        pass

    def with_tmpfile(self):
        pass

