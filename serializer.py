__author__ = 'stanley, jacob'

import marshal
import binascii


class Serializer(object):

    def __init__(self):
        pass

    class BaseObj(object):

        def __init__(self):
            pass

        def isbinary(self, str):
            """Check if the string is binary, returns true or false."""
            try:
                # Try to parse string as binary string
                int(str, 2)
            except ValueError:
                # If the parse failed with ValueError, string is not binary
                return False
            except Exception as e:
                # If unexpected error occurred, raise it
                raise e
            # If the parse succeeded, string is binary
            return True

        def encode_binary(self, str):
            """Encode a string to binary, return encoded string."""
            return bin(int(binascii.hexlify(str.encode('utf-8')), 16))

        def can_binary(self, str):
            """Check if the passed 'string' can convert to binary, return true or false."""
            # Not sure how to do this..so for now I will just try to encode it and return false if fails.
            try:
                self.encode_binary(str)
            except Exception:
                return False
            return True

        def key_for(self, key):
            if self.can_binary(key) and not self.isbinary(key):
                return self.encode_binary(key)
            else:
                return key

        def dump(self, value):
            return value

        def load(self, value):
            return value

    class Default(BaseObj):
        """Default serializer which converts"""

        def __init__(self):
            Serializer.BaseObj.__init__(self)
            pass

        def key_for(self, key):
            """Transform the key to a string."""
            return str(key)

        def dump(self, value):
            """Serialize a value and return the binary string."""
            return marshal.dumps(value)

        def load(self, value):
            """Parse a binary string and return the parsed value."""
            return marshal.loads(value)
