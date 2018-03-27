
class Identity(object):

    def __init__(self, name, *args, **kwargs):

        self.name = name
        # TODO : should probably copy these
        self.args = tuple(args)
        self.kwargs = frozenset(kwargs)
        
        # Compute hash
        self.args_hash = hash(self.args)
        self.kwargs_hash = hash(self.kwargs)

    def __hash__(self):
        '''
        Identity is based on name and hash of arguments
        '''
        id_tuple = (self.name, self.args_hash, self.kwargs_hash)
        return hash(id_tuple)


class Returner(Identity):

    def __init__(self, name, some_object):

        super(Returner, self).__init__(name)

    def def extend_instance(obj, cls):
    """Apply mixins to a class instance after creation"""
    base_cls = obj.__class__
    base_cls_name = obj.__class__.__name__
    obj.__class__ = type(base_cls_name, (base_cls, cls),{})

