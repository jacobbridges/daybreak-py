__author__ = 'stanley'

from serializer import *
from journal import *
from format import *
from collections import defaultdict
import mutex


class DB(object):

    # Private
    __databases = []

    # Private
    __databases_mutex = mutex.mutex()

    def __init__(self, file_name, options=None):
        """
        :param file_name: Name of the database journal file.
        :param options: Dictionary with the following keys:
                        serializer - class to serialize the database data
                        format - class to format the data in the journal
                        default - default value for any key in the database
        """
        # If options are not set, fill in with default values
        if options is None:
            options = {
                'serializer': Serializer.Default,
                'format': Format,
                'default': str
            }
        else:
            if 'serializer' not in options:
                options['serializer'] = Serializer.Default
            if 'format' not in options:
                options['format'] = Format
            if 'default' not in options:
                options['default'] = str
            else:
                options['default'] = lambda: options['default']

        # Set instance variables
        self.serializer = options['serializer']()
        self.table = defaultdict(options['default'])

        # Create the journal and block function to send to journal
        def block(record):
            if not record:
                self.table.clear()
            elif len(record) == 1:
                self.table.delete(record[0])
            else:
                self.table[record[0]] = self.serializer.load(record[-1])
        self.journal = Journal(file_name, options['format'](), self.serializer, block)

        # Synchronization Mutex stuff
        self.__databases_mutex.lock(self.__databases.append, self)
        self.__databases_mutex.unlock()

    def file(self):
        """Returns database file name."""
        return self.journal.file.name

    def default(self, key=None):
        """Return default value belonging to key."""
        return self.table.default_factory(self.serializer.key_for(key))

    def __getitem__(self, key):
        """Retrieve a value at key from the database."""
        # Allows the DB[key] syntax
        return self.table[self.serializer.key_for(key)]
    get = __getitem__

    def __setitem__(self, key, value):
        """Set a key in the database to be written at some future date."""
        key = self.serializer.key_for(key)
        self.journal << [key, value]
        self.table[key] = value
    set = __setitem__

    def set_flush(self, key, value):
        """Flushes data immediately to disk."""
        self.set(key, value)
        self.flush()
        return value

    def __delitem__(self, key):
        """Delete a key from the database."""
        key = self.serializer.key_for(key)
        self.journal << [key]
        del self.table[key]
    delete = __delitem__

    def delete_flush(self, key):
        """Immediately delete the key on disk."""
        value = self.table[key]
        self.delete(key)
        self.flush()
        return value

    def update(self, d):
        """Update database with dict (Fast batch update)."""
        sd = {}
        for key, value in d:
            sd[self.serializer.key_for(key)] = value
        self.journal << sd
        self.table.update(sd)
        return self

    def update_flush(self, d):
        """Update database with dict (Fast batch update)."""
        self.update(d)
        self.flush()

    def has_key(self, key):
        """Does this db have this key?"""
        return self.serializer.key_for(key) in self.table
    include = has_key
    is_member = has_key

    def has_value(self, value):
        """Does this db have this value?"""
        return value in self.table.values()

    def size(self):
        """Return the number of stored items."""
        return len(self.table)

    def bytesize(self):
        """Utility method that will return the size of the database in bytes."""
        return self.journal.bytesize()

    def logsize(self):
        """Counter of how many records are in the journal."""
        return self.journal.size

    def is_empty(self):
        """Return true if database is empty."""
        return not self.table

    def __iter__(self):
        """Iterate over the key, value pairs in the database."""
        return self.table.__iter__()

    def keys(self):
        """Return the keys in the db."""
        return self.table.keys()

    def flush(self):
        """Flush all changes to disk."""
        self.journal.flush()
        return self

    def load(self):
        """Sync the database with what is on disk."""
        self.journal.load()
        return self
    sunrise = load

    def lock(self):
        """Lock the database for an exclusive commit across processes and threads."""
        return self.journal.lock()

    def synchronize(self):
        """Synchronize access to the database from multiple threads."""
        # Python Queues are naturally thread safe so no reason to have this function.
        # Not that I know of...
        pass

    def clear(self):
        """Remove all keys and values from the database."""
        self.table.clear()
        self.journal.clear()
        return self

    def compact(self):
        """Compact the database to remove stale commits and reduce the file size."""
        self.journal.compact()
        return self

    def close(self):
        """Close the database for reading and writing."""
        self.journal.close()
        self.table.clear()
        self.__del__()

    def closed(self):
        """Check to see if we've already closed the database."""
        return self.journal.closed()

    def __del__(self):
        """A handler that will ensure that databases are closed and synced when the current process exits."""
        while True:
            db = self.__databases[-1:]
            if not db:
                break
            else:
                db = db[0]
            self.__databases_mutex.lock(self.__databases.pop, -1)
            self.__databases_mutex.unlock()
            print "Daybreak database {} was not closed, state might be inconsistent".format(db.file())
            try:
                db.close()
            except Exception as ex:
                print "Failed to close daybreak database: {}".format(ex.message)


