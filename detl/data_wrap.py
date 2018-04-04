class DatasetWrapper(object):
    '''Necessary because the output of get_dataset extends dict
    AND I can't set attributes of numpy arrays'''
    def __init__(self, data):
        self.data = data


