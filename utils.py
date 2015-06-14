__author__ = 'jacob'


def bytesize(s):
    """Returns the number of bytes in a string."""
    return len(s.decode('utf-8').encode('utf-8'))
