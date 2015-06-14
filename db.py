__author__ = 'stanley'

from serializer import *
from journal import *
from format import *
from collections import defaultdict


class Daybreak(object):

    class DB(object):

        def __init__(self, fileName, dbblock, options=None):
            if options is None:
                options = {
                    'serializer': Serializer.Default,
                    'format': Format,
                    'default': lambda: ''
                }
            self.serializer = options['serializer']()
            self.table = defaultdict(options['default'])
            self.journal = Journal(fileName, options['format'](), self.serializer, self.block)

            self.default = dbblock if dbblock else options['default']

            # Synchronization Mutex stuff

        def block(self, record):
            if not record:
                pass
            elif len(record) == 1:
                pass
            else:
                pass

        def file(self):
            return self.journal.file

        def default(self, key=None):
            return self.table.default(self.serializer.key_for(key))

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
            value = self.delete(key)
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
            return self.journal.size()

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

