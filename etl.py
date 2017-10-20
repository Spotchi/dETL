import numpy as np


class ETL(object):
    
    def __init__(self, fn, *args, **kwargs):
        self.fn = fn
        self.args = args
        self.kwargs = kwargs
        self.evaluated = False

    def forward(self):
    '''
    Two hashes of the same operation can be different because at some point in the hierarchy, a function has been
    called with a value that has a different hash than previously.
    The config hash is useful because it allows to map the config to the result. The result can then be used for the next step 
    of the pipeline.
    When calling forward, the function checks the hashes of the inputs and then check if they are in the database. If one of them isn't,
    we have to go back there and compute it.
    '''
        #if hashed in db:
        #    return load(hashed)
        eval_args = [evaluate(item) for item in self.args]
        eval_kwargs = {k:evaluate(item) for k,item in self.kwargs.items()}
        self.evaluated = True
        return self.fn(*eval_args, **eval_kwargs)

    def config_hash(self, eval_args, eval_kwargs):
        ev_dict = {'fn':self.fn.__name__, 'args':eval_args, 'kwargs':eval_kwargs}
        self.config_hash = hashlib.sha256(json.dumps(ev_dict, sort_keys=True).encode())
        return self.hash
        
    def load(self):
        pass

    def check_computed(self):
        pass

def evaluate(obj_or_val):
    if isinstance(obj_or_val, ETL):
        return obj_or_val.forward()
    return obj_or_val

def register_ETL(fn):
    def inner_fn(*args, **kwargs):
        return ETL(fn, *args, **kwargs)
    return inner_fn


@register_ETL
def normalize(x):
    return x-x.mean()

@register_ETL
def divide_etl(x):
    return x/5.0

@register_ETL
def returner(x):
    return x


x = returner(np.array([2.0, 5.0, 42.0]))
x1 = normalize(x)
x2 = divide_etl(x)

print(x2.forward())

