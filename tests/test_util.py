def save_int(int_num, filepath):
    
    assert type(int_num) is int
    with open(filepath, 'w') as fd:
        fd.write(str(int_num))

def load_int(filepath):

    with open(filepath, 'r') as fd:
        num = int(fd.read())

    return num
