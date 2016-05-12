"""
format.py

classes:
  BaseFormat
  DefaultFormat
"""
from __future__ import absolute_import

import json

from binascii import crc32
from abc import ABCMeta, abstractmethod
from struct import pack, unpack

from StringIO import StringIO
from toolz import first


####################################################################################################
# Custom Exceptions

class FormatException(Exception):
    pass


####################################################################################################
# Classes

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


class DefaultFormat(BaseFormat):

    # Magic string of the file header
    MAGIC = bytearray('DAYBREAK')

    # Database file format version
    VERSION = 1

    # Special value size used for deleted records
    DELETE = (1 << 32) - 1

    def read_header(self, stream):
        """
        Verify the header of a database file.

        Verifies that a file is a Daybreak database and the version is compatible.

        Parameters
        ----------
        :param stream: File handler for the database file

        Types
        -----
        :type stream: file

        Returns
        -------
        :returns: Nothing
        :rtype:   None

        Raises
        ------
        :raises: FormatException
        """

        # Move the file cursor to the beginning of the file
        stream.seek(0)

        # Verify the Daybreak magic value
        if stream.read(len(self.MAGIC)) != self.MAGIC:
            raise FormatException('Not a Daybreak database')

        # Verify the Daybreak database version
        version = first(unpack('!H', stream.read(2)))
        if version != self.VERSION:
            raise FormatException("Expected database version {}, got {}".format(self.VERSION, version))

    def create_header(self):
        """
        Create the header string for a Daybreak database.

        Returns
        -------
        :return: Header string for a Daybreak database.
        :rtype: bytearray
        """
        return bytearray(self.MAGIC) + bytearray(pack('!H', self.VERSION))

    def serialize(self, data):
        """
        Transform a record object into a serialized string.

        Parameters
        ----------
        :param data: Tuple with either (key, value), or just (value)

        Types
        -----
        :type data: tuple

        Returns
        -------
        :return: Record as serialized string.
        :rtype: bytearray
        """

        # Initialize the serialized string
        serialized = bytearray()

        # Cast "key" to bytearray
        key_as_bytes = bytearray(str(data[0]))

        # If the data tuple only contains one element, create a "delete" record
        if len(data) is 1:

            # Add the delete serialization to the serialized string
            serialized += bytearray(pack('!II', len(key_as_bytes), self.DELETE)) + key_as_bytes

        # If the data tuple contains 2 elements, create a "new" record
        else:

            # If "value" is a dictionary, convert to json
            if isinstance(data[1], dict):
                value_as_bytes = bytearray(json.dumps(data[1]))

            # If "value" is a string, convert to bytes
            elif isinstance(data[1], basestring):
                value_as_bytes = bytearray(str(data[1]))

            # If value is anything else, raise an error
            else:
                raise FormatException('Unexpected value type -- Daybreak only accepts strings and '
                                      'dictionaries.')
            # Add the new serialization to the serialized sting
            serialized += (bytearray(pack('!II', len(key_as_bytes), len(value_as_bytes))) +
                           key_as_bytes +
                           value_as_bytes)

        # Return the serialized string with an appended 32-bit CRC
        return serialized + bytearray(self.crc32(serialized))

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
        return pack('!I', crc32(str(s)) % (1 << 32))
