"""
format.py

Description
"""
from abc import ABCMeta, abstractmethod
from struct import pack, unpack
from StringIO import StringIO
from toolz import first
from binascii import crc32
import json


class BaseFormat(object):
    """
    Abastract base class which any formatter should extend
    """
    __metaclass__ = ABCMeta

    @abstractmethod
    def read_header(self, stream):
        """
        Reads and parses the database's header.
        :param stream: Python file buffer (must have "read" method)
        """
        pass

    @abstractmethod
    def create_header(self):
        """
        Composes a daybreak database's header and returns it as a string.
        :return: Daybreak database header as string
        """
        pass

    @abstractmethod
    def serialize(self, data):
        """
        Serializes data into a compatible database record string.
        :param data: Must be a list with one or two items (1=delete, 2=[key, value])
        :return: Data as compatible database record string
        """
        pass

    @abstractmethod
    def deserialize(self, string):
        """
        Parses a database record string into a Python list.
        :param string: Database record string (serialized)
        :return: Database record string as Python list
        """
        pass


class FormatException(Exception):
    pass


class DefaultFormat(BaseFormat):

    # Magic string of the file header
    MAGIC = bytearray('DAYBREAK')

    # Database file format version
    VERSION = 1

    # Special value size used for deleted records
    DELETE = (1 << 32) - 1

    def read_header(self, stream):
        stream.seek(0)
        if stream.read(len(self.MAGIC)) != self.MAGIC:
            raise FormatException('Not a Daybreak database')
        version = first(unpack('!H', stream.read(2)))
        if version != self.VERSION:
            raise FormatException("Expected database version {}, got {}".format(self.VERSION, version))

    def create_header(self):
        return bytearray(self.MAGIC) + bytearray(pack('!H', self.VERSION))

    def serialize(self, data):
        data[0] = bytearray(str(data[0]))
        if len(data) == 1:
            record = bytearray(pack('!II', len(data[0]), self.DELETE)) + data[0]
        else:
            if data[1] is dict:
                data[1] = json.dumps(data[1])
            data[1] = bytearray(str(data[1]))
            record = bytearray(pack('!II', len(data[0]), len(data[1]))) + data[0] + data[1]
        return record + bytearray(self.crc32(record))

    def deserialize(self, string):
        string = StringIO(string)
        while string.tell() < string.len:
            meta = string.read(8)
            key_size, value_size = unpack('!II', meta)
            data_size = key_size
            if value_size != self.DELETE:
                data_size += value_size
            data = string.read(data_size)
            if string.read(4) != self.crc32(meta + data):
                raise FormatException("CRC mismatch: your data might be corrupted!")
            if value_size == self.DELETE:
                yield [data[:key_size]]
            else:
                value = data[-value_size:]
                try:
                    value = eval(value)
                except:
                    pass
                yield [data[:key_size], value]

    def crc32(self, s):
        return pack('!I', crc32(str(s)) & 0xffffffff)
