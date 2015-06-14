__author__ = 'stanley, jacob'

import os
from .utils import bytesize
from .FileLock import FileLock
from Queue import Queue
from threading import Thread


class Journal(Queue):
    """
    The Journal is a thread-safe queue which appends to a file.
    """

    def __init__(self, file_path, storage_format, serializer, block):
        """
        :param file_path: Path to journal file
        :param storage_format: Format to save the db contents to the journal file
        :param serializer:
        :param block:
        """

        # Call to parent class constructor
        Queue.__init__(self)

        # Set some properties from the instantiation call
        self.file_path = file_path
        self.format = storage_format
        self.serializer = serializer
        self.emit = block

        # Open the journal file
        self.open()

        # Start the journal worker
        self.worker = Thread(name='journal_worker', target=self.worker_run)

        # Load any current journal entries
        self.load()

    def __lshift__(self, record):
        """Puts an item in the queue via journal << record."""
        self.put(record, block=False)

    def closed(self):
        """Is the journal closed?"""
        return self.file.closed

    def close(self):
        """Clear the queue and close the file handler."""
        with self.mutex:
            self.queue.clear()
        self.worker.join()
        self.file.close()

    def load(self):
        """Load new journal entries."""
        self.file.flush()
        self.join()
        self.replay()

    def lock(self, code='None'):
        """Lock the logfile across thread and process boundaries."""
        self.flush()
        fl = FileLock(self.file_path)
        with fl:
            self.replay()
            result = eval(code)
            self.flush()
            return result

    def clear(self):
        """Clear the database log and yield."""
        self.flush()
        with self.TempFile(self.file_path) as temp_file:
            temp_file.write(self.format.header)
            temp_file.close()
            fl = FileLock(self.file_path)
            with fl:
                assert fl.is_locked
                os.rename(temp_file.name, self.file_path)
        self.open()

    def flush(self):
        """Flush all write buffers to the file if applicable."""
        self.join()
        self.file.flush() if not self.closed() else None

    def compact(self):
        """Compact the logfile to represent the in-memory state."""
        self.load()
        with self.TempFile(self.file_path) as temp_file:
            if self.pos == self.file.write(self.dump(self.format.header)):
                return self
            fl = FileLock(self.file_path)
            with fl:
                assert fl.is_locked
                if self.pos != 0:
                    temp_file.write(self.read())
                    temp_file.close()
                    os.rename(temp_file.name, self.file_path)
        self.open()
        self.replay()

    def bytesize(self):
        """Return byte size of journal."""
        return os.stat(self.file_path).st_size

    def replay(self):
        """Emit records as we parse them."""
        buf = self.read()
        self.size += self.format.parse(buf, self.emit)

    def open(self):
        """Open or reopen file."""
        self.file.close() if self.file else None
        self.file = open(self.file_path, 'ab+')
        stat = os.stat(self.file_path)
        self.inode = stat.st_ino
        self.write(self.format.header) if stat.st_size == 0 else None
        self.pos = 0

    def read(self):
        """Read new file content"""
        fl = FileLock(self.file_path)
        with fl:
            assert fl.is_locked
            if not self.pos:
                self.file.seek(0)
                self.format.read_header(self.file)
                self.size = 0
                self.emit.call(None)
            else:
                self.file.seek(self.pos)
            buf = self.file.read()
            self.pos = self.file.tell()
            return buf

    def dump(self, records, dump=''):
        """Return database dump as string."""
        for record in records:
            record[1] = self.serializer.dump(record[-1])
            dump += self.format.dump(record)
        return dump

    def worker_run(self):
        """Worker thread - Write any database records added to the queue into the journal."""
        try:
            while True:
                record = self.get()
                tries = 0
                while tries <= 3:
                    try:
                        if isinstance(record, dict):
                            self.write(self.dump(record))
                            self.size += len(record)
                        else:
                            record[1] = self.serializer.dump(record[-1]) if record.size > 1 else  None
                            self.write(self.format.dump(record))
                            self.size += 1
                    except Exception as ex:
                        tries += 1
                        print "Daybreak worker, try {}: {}".format(tries, ex.message)
                        if tries <= 3:
                            continue
                        else:
                            raise ex
                    finally:
                        self.task_done()
                    break
        except Exception as ex:
            print "Daybreak worker terminated: {}".format(ex.message)

    def write(self, dump):
        """Write data to output stream and advance self.pos."""
        fl = FileLock(self.file_path)
        with fl:
            self.file.write(dump)
            self.file.flush()
        if self.pos and (self.file.tell() == self.pos + bytesize(dump)):
            self.pos = self.file.tell()

    class TempFile(object):
        """
        Creates a temporary file in the same directory as the journal file.
        """
        from uuid import uuid1
        from base64 import b32encode

        def __init__(self, journal_path):
            """
            :param journal_path: Path to journal file
            """
            self.journal_path = journal_path

        def __enter__(self):
            path = ''.join([self.journal_path, self.b32encode(self.uuid1())[:64]])
            self.file = open(path, 'wb')
            return self.file

        def __exit__(self, type, value, traceback):
            self.file.close() if not self.file.closed else None
            os.unlink(self.file.path) if os.path.isfile(self.file.path) else None

