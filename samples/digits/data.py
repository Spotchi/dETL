from detl.processor import load_and_save, identity_wrapper
import matplotlib.pyplot as plt
import random
from sklearn.datasets import load_digits
from sklearn.model_selection import train_test_split
import numpy as np

@identity_wrapper()
def get_dataset(n_class=10):
    digits = load_digits(n_class=n_class)
    return digits

# @visualize(get_dataset)
def show_sample_images(data):
    digits = data.data
    num = random.randrange(len(digits.images))
    plt.gray()
    plt.matshow(digits.images[num])
    plt.show()


def np_load(fd):
    full_filename = fd + '.npy'
    return np.load(full_filename)

def np_save(results, file_path):
    np.save(file_path, results)



@load_and_save(np_load, np_save, unpack=4)
def split(digits, seed=42, test_size=.3):
    X_train, X_test, y_train, y_test = train_test_split(digits.images, digits.target, test_size=test_size, random_state=seed)

    return X_train, X_test, y_train, y_test
    
