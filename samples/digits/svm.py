from detl.processor import load_and_save, identity_wrapper, Processor, change_state
from sklearn import svm, metrics
import numpy as np
import pandas as pd
from io_utils import pd_to_csv


def flatten_sample(images):
    n_samples = len(images)
    return images.reshape((n_samples, -1))


def save_num(int_num, filepath):
    
    with open(filepath, 'w') as fd:
        fd.write(str(int_num))

def load_num(filepath):

    with open(filepath, 'r') as fd:
        num = float(fd.read())

    return num


class SVMClassifier(Processor):

    def __init__(self, *args, **kwargs):

        super(SVMClassifier, self).__init__(*args, **kwargs)
        # TODO : how to pass the default arguments of svm to the processor class?
        self.svm_model = svm.SVC(*args, **kwargs)

    @change_state
    def fit(self, data, target):

        self.svm_model.fit(flatten_sample(data), target)

    @identity_wrapper()
    def predict(self, data):
        predicted = self.svm_model.predict(flatten_sample(data))
        return predicted


@load_and_save(pd.read_csv, pd_to_csv)
def confusion_matrix(expected, predicted):
        conf_mat = metrics.confusion_matrix(expected, predicted)
        return pd.DataFrame(conf_mat)

@load_and_save(load_num, save_num)
def accuracy(expected, predicted):
    return metrics.accuracy_score(expected, predicted)


