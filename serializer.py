__author__ = 'stanley'

import marshal

class Serializer(object):

    def __init__(self):
        pass

    class BaseObj(object):

        def __init__(self):
            pass

        def isbinary(self, str):

            # Check if the string is binary
            # return true or false

            return False

        def encode_binary(self, str):

            # Encode a string to binary
            # return encoded string
            return str

        def can_binary(self, str):

            # Check if the passed 'string' can convert to binary

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

    # Default serializer which converts
    class Default(BaseObj):

        def __init__(self):
            pass

        # Transform the key to a string
        def key_for(self, key):
            return str(key)

        # Serialize a value
        def dump(self, value):
            return marshal.dumps(value)

        # Parse a value
        def load(self, value):
            return marshal.loads(value)

if __name__ == "__main__":
    d = Serializer.Default()
    print d.dump('afdvscds')
