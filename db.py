__author__ = 'stanley'

from serializer import *

class Daybreak(object):

    def __init__(self, fileName, **options):
        self.serializer = (options['serializer'] or Serializer.Default)()

        self.file = fileName

