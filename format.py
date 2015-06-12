__author__ = 'stanley'
from struct import pack, unpack
import sys
from exception import FormatException
# Database format serializer and deserializer.

class Format:
    # Magic string of the file header
    MAGIC = 'DAYBREAK'

    # Database file format version
    VERSION = 1

    # Special value size used for deleted records
    DELETE = (1 << 32) - 1

    def __init__(self):
        pass

    # Read database header from input stream
    # @param [#read] input the input stream
    # @return void
    def read_header(self, input):
        if input.read(sys.getsizeof(Format.MAGIC.bytesize)) != Format.MAGIC:
            raise FormatException('Not a Daybreak database')

        version = unpack(input.read(2), 'n').index(0)
        # To do: is ruby equivalent of unpacking relevant here?

        if version != Format.VERSION:
            raise FormatException("Expected database version #{VERSION}, got #{ver}")

    # Return database header as string
    # @return [String] database file header
    def header(self):
        # Still not sure how the pack params work
        return Format.MAGIC + pack([Format.VERSION], 'n')

    # Serialize record and return string
    # @param [Array] record an array with [key, value] or [key] if the record is deleted
    # @return [String] serialized record
    def dump(self, record):
        pass

    # Deserialize records from buffer, and yield them.
    # @param [String] buf the buffer to read from
    # @yield [Array] blk deserialized record [key, value] or [key] if the record is deleted
    # @return [Fixnum] number of records
    def parse(self, buf):
        pass

    # Compute crc32 of string
    # @param [String] s a string
    # @return [Fixnum]
    def crc32(self, s):
        pass
