"""
journal.py

classes:
  Journal
"""
import os

from Queue import Queue
from threading import Thread

from FileLock import FileLock


####################################################################################################
# Classes


class Journal(Queue):
    """
    Class for interacting with the database file (or "journal") via a thread-safe Queue.
    """

    def __init__(self, file_path, formatter):
        """Initialize the journal with a database file, formatter object, and thread."""
        Queue.__init__(self)
        self.file_path = file_path
        self.formatter = formatter
        self.file = None
        self.inode = None
        self.count = 0
        self.byte_size = 0
        self.pos = 0
        self.open()
        self.thread = Thread(target=self.worker)
        self.thread.daemon = True
        self.thread.start()

    def __lshift__(self, record):
        """
        Send record to be processed by the internal thread.
        :param record: Python list with either [key, value] or [key]
        """
        self.put(record)

    def clear(self):
        """Clear the journal file's contents."""
        self.file.seek(0)
        self.file.truncate()
        self.file.close()
        self.open()

    def closed(self):
        """Check if the journal file is closed."""
        return self.file.closed

    def close(self):
        """Clear the queue, stop the thread, and close the journal file."""
        with self.mutex:
            self.queue.clear()
        self.put(None)
        self.file.close()

    def load(self):
        """
        Load all the records from the journal file and return the non-deleted ones.
        :return: list of records
        """
        if len(self.queue) > 0:
            self.join()
        self.pos = 0
        data = filter(lambda x: len(x) > 1, list(self.formatter.deserialize(self.read())))
        self.count = len(data)
        return data

    def opened(self):
        """Check if the journal file is open."""
        return not self.file.closed

    def open(self):
        """Open the journal file, or create a new journal file if it does not exist."""
        self.file = open(self.file_path, 'ab+')
        stat = os.stat(self.file_path)
        self.inode = stat.st_ino
        if stat.st_size == 0:
            self.write(self.formatter.create_header())
        self.pos = self.file.tell()

    def read(self):
        """
        Read the journal file from previous position, returning the contents as bytes.
        :return: The data from file as a byte array.
        """
        with FileLock(self.file_path):
            self.file.seek(self.pos)
            if self.pos is 0:
                self.formatter.read_header(self.file)
            buf = self.file.read()
        self.pos = self.file.tell()
        return bytearray(str(buf))

    def write(self, string):
        """
        Write some data to the journal file.
        :param string: Serialized data
        """
        string = bytearray(str(string))
        with FileLock(self.file_path):
            self.file.write(string)
            self.file.flush()
        self.pos = self.file.tell()

    def worker(self):
        """Threaded function which processes records put in the internal queue."""
        while True:
            record = self.get()
            if record is None:
                self.task_done()
                break
            self.write(self.formatter.serialize(record))
            if record > 1:
                self.count += 1
            self.task_done()
