"""
journal.py

Journal description...
"""
import os
import toolz
from Queue import Queue
from FileLock import FileLock
from threading import Thread


class Journal(Queue):
    """
    class Journal

    """
    def __init__(self, file_path, formatter):
        print 'initialize journal'
        Queue.__init__(self)
        self.file_path = file_path
        self.file = None
        self.inode = None
        self.count = 0
        self.byte_size = 0
        self.pos = 0
        self.formatter = formatter

        print 'opening journal file'
        self.open()

        self.thread = Thread(target=self.worker)
        self.thread.start()

    def __lshift__(self, record):
        self.put(record)

    def clean(self):
        pass

    def closed(self):
        return self.file.closed

    def close(self):
        with self.mutex:
            self.queue.clear()
        self.put(None)
        self.file.close()

    def load(self):
        print 'loading db file'
        if len(self.queue) > 0:
            print "{} items in the queue left to process".format(len(self.queue))
            self.join()
        self.pos = 0
        self.count = 0
        return filter(lambda x: len(x) > 1, list(self.formatter.deserialize(self.read())))

    def opened(self):
        return not self.file.closed

    def open(self):
        self.file = open(self.file_path, 'ab+')
        stat = os.stat(self.file_path)
        self.inode = stat.st_ino
        if stat.st_size == 0:
            self.write(self.formatter.create_header())
        self.pos = self.file.tell()

    def read(self):
        with FileLock(self.file_path):
            self.file.seek(self.pos)
            if self.pos is 0:
                self.formatter.read_header(self.file)
            buf = self.file.read()
        self.pos = self.file.tell()
        return bytearray(str(buf))

    def write(self, string):
        string = bytearray(str(string))
        with FileLock(self.file_path):
            self.file.write(string)
            self.file.flush()
        self.pos = self.file.tell()

    def worker(self):
        while True:
            record = self.get()
            if record is None:
                self.task_done()
                break
            self.write(self.formatter.serialize(record))
            if record > 1:
                self.count += 1
            self.task_done()
