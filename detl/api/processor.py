

class Processor(object):

    def __init__(self, *args, **kwargs):
        '''
        A class that processes some computations and includes a save and load function
        This class is merely a different way of implementing the ETL class. Here the functions and arguments are defined by the programmer coding directly in the extending class
        '''
        db = db_context.get_db()

        class_name = self.__class__.__name__
        self.identity = Identity(class_name, *args, **kwargs)
        if db is not None:
            fd = db.find(self.identity)
            if fd is None:
                db.insert(self, None, save_data=False)

    def __id_hash__(self):
        return self.identity.__id_hash__()
# TODO : no need to implement this? Just need to make sure that the state of the object that changes is serialized
def change_state(fn, load_func=None, save_func=None):
    @wraps(fn)
    def inner_fn(self, *args, **kwargs):
        assert issubclass(type(self), Processor)
        class_name = self.__class__.__name__
        class_method_name = fn.__name__
        
        old_identity = self.identity

        obj_args = (old_identity,)+tuple(args)
        self.identity = Identity(class_name+class_method_name, *obj_args, **kwargs)
        
        db = db_context.get_db()
        if db is None:
            get_args = [get_data(el) for el in args]
            get_kwargs = {k:get_data(v) for k,v in kwargs.items()}
            return fn(self, *get_args, **get_kwargs)

        fd = db.find(self.identity)
        if fd is None:
            if save_func is not None and load_func is not None:
                print('save_func defined')
                get_args = [get_data(el) for el in args]
                get_kwargs = {k:get_data(v) for k,v in kwargs.items()}

                results = fn(self, *get_args, **get_kwargs)
                db.insert(self, save_func, save_data=True)
            else:
                print('save_func not defined')
                db._insert(self.identity, None, None, save_data=False)
        else:
            # TODO : We should account for the case when the state changes AND we return some data. In that case both need to be identified / saved
            pass
            #results = fn(self, *args, **kwargs)
            #db._insert(self.identity, None, None, save_data=False)
        get_args = [get_data(el) for el in args]
        get_kwargs = {k:get_data(v) for k,v in kwargs.items()}


        return fn(self, *get_args, **get_kwargs)
    return inner_fn


