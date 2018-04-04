from detl.data_wrap import DatasetWrapper
def save_int(int_num, filepath):
    
    assert type(int_num.data) is int
    with open(filepath, 'w') as fd:
        fd.write(str(int_num.data))

def load_int(filepath):

    with open(filepath, 'r') as fd:
        num = int(fd.read())

    return DatasetWrapper(num)
