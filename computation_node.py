from flask import Flask
import db
from etl import ETL

app = Flask(__name__)

@app.route('/compute', methods = ['POST'])
def hello_world():
    return 'Needs to be implemented, returns a status saying the code is executing'