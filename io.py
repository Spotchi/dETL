import pandas as pd
import numpy as np
import json

def pd_to_csv(df, filename):
    df.to_csv(filename)

def json_dump(di, filename):
    with open(filename, 'w') as outfile:
        json.dump(di, outfile)


