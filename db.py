__author__ = 'stanley'

from serializer import *
from journal import *
from format import *

class Daybreak(object):

    def __init__(self, fileName, dbblock, **options):
        self.serializer = (options['serializer'] or Serializer.Default)()
        self.table = {}
        self.journal = Journal(fileName, (options['format'] || Format)(), self.serializer, self.block)

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