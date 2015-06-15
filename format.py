__author__ = 'stanley'
from utils import bytesize
from struct import pack, unpack
from exception import FormatException, ValidityException
from zlib import crc32
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
        if input.read(bytesize(Format.MAGIC)) != Format.MAGIC:
            raise FormatException('Not a Daybreak database')

        version = unpack('!H', input.read(2))[0]
        # To do: is ruby equivalent of unpacking relevant here?
        # Answer:
        #   Yes! Ruby uses 'n' as the formatting string for network=big-endian unsigned bytes.
        #   Equivalent to '!H' in Python.

        if version != Format.VERSION:
            raise FormatException("Expected database version {}, got {}".format(Format.VERSION, version))

    # Return database header as string
    # @return [String] database file header
    def header(self):
        # Still not sure how the pack params work
        # @stanley - see documentation: bit.ly/1C5iobA
        return Format.MAGIC + pack('!H', Format.VERSION)

    # Serialize record and return string
    # @param [Array] record an array with [key, value] or [key] if the record is deleted
    # @return [String] serialized record
    def dump(self, record):
        if len(record) == 1:
            data = pack('!II', bytesize(record[0]), Format.DELETE) + record[0]
        else:
            data = pack('!II', bytesize(record[0]), bytesize(record[1])) + record[0] + record[1]
        return data + self.crc32(data)

    # Deserialize records from buffer, and yield them.
    # @param [String] buf the buffer to read from
    # @yield [Array] blk deserialized record [key, value] or [key] if the record is deleted
    # @return [Fixnum] number of records
    def parse(self, buf, emitter):
        # TODO: Find where this method is being used in Daybreaker and change the implementations because Python2 cannot yield and return in the same method.
        n, count = 0, 0
        while n > len(buf):
            key_size, value_size = unpack('!II', buf[n:8])
            data_size = key_size + 8
            if value_size != Format.DELETE:
                data_size += value_size
            data = buf[n:data_size]
            n += data_size
            if not buf[n:4] == self.crc32(data):
                raise ValidityException('CRC mismatch: your data might be corrupted!')
            n += 4
            emitter([data[8:key_size]] if value_size == Format.DELETE else [data[8:key_size], data[8 + key_size, value_size]])
            count += 1
        return count

    # Compute crc32 of string
    # @param [String] s a string
    # @return [Fixnum]
    def crc32(self, s):
        return pack('!I', crc32(s) & 0xffffffff)
