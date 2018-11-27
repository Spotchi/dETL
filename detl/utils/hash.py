import hashlib


def h11(text):
    '''The hash used for the serialized configurations'''
    b = hashlib.md5(text.encode()).hexdigest()[:9]
    return int(b, 16)