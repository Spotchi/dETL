# dETL

dETaiL is a project that aims at taking away the hassle from tuning different combinations of ETL pipelines

It increases speed of computation by simply loading the results that were previously computed from disk.

DISCLAIMER : The code in this doesn't deal with side effects so if some variable involved has been
modified between two identification the entry in the database might be wrong.

## Setup

Set up MongoDB (works on Ubuntu 16.04):
----------------------------

Follow the instructions in the following tutorial

https://www.howtoforge.com/tutorial/install-mongodb-on-ubuntu-16.04/#install-mongodb-on-ubuntu-


Install library using pip
-------------------------

Pip requirements coming soon

Go to the detl project root and type :

pip install -e .

## Usage

Let's say you have some data x in the form of a pandas dataframe. You might want to apply the
following transformation that will multiply it by 2.

If you wanted to save the output and know exactly how it was computed, you could save it to a csv
file and give it a specific name. Our approach is to associate a load and a save function to the
transformation as follows.

```python
import pandas as pd
from detl.processor import load_and_save
from detl.identity import identify

@load_and_save(pd.read_csv, pd_to_csv)
def multiply_by(x, y):
    return x * y

def pd_to_csv(df, filename):
    df.to_csv(filename)
```

Now if we execute the operation once, the multiplication will be saved to the database, but we need
to identify the source first.

```df = pd.DataFrame(np.arange(12).reshape(3,4),
                  columns=['A', 'B', 'C', 'D'])
df_ided = identify(df, 'data source')

mult = multiply_by(df_ided, 5)
```

The second time we run it, if the arguments are the same the result will be loaded from the
database.

### Configure database connection

It is now necessary to specify what database we're connecting to using a detl.mydb.MyDb object

We can manage what database the results are loaded and whether they are loaded at all using context
managers.
```python
from detl.mydb import db_client
from samples.sample1 import PreProcessor

sample_db = db_client()
with sample_db.as_default():

    df = pd.DataFrame(np.arange(12).reshape(3,4),
                      columns=['A', 'B', 'C', 'D'])
    df_ided = identify(df, 'data source')

    preprocessor = PreProcessor(num_mul=50)

    mult = preprocessor.processor_2(df_ided)
```
The code above creates a connection using the default config file for the db, configs/db.json

Planned features
----------------
* Lazy evaluation of decorated functions using a lazy flag context manager, and adding a .forward()
  method to outputs of all decorated methods
* Template for datasets where the decorated function are applied to every element of a dataset
* Browse function
* Template CLIs that take function name as argument
* Visualize computation tree
* Test to check that saved items correspond to function
* Computer vision and time series use cases
