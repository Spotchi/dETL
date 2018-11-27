import pandas as pd
from data import get_dataset, split
from detl.store.mydb import db_client
from svm import SVMClassifier, confusion_matrix, accuracy

with db_client().as_default():

    
    digits = get_dataset()

    X_train, X_test, y_train, y_test = split(digits)

    classifier = SVMClassifier()
    
    classifier.fit(X_train, y_train)

    pred = classifier.predict(X_test)
    
    print(confusion_matrix(y_test, pred).data)
    print('Accuracy', accuracy(y_test, pred).data)
