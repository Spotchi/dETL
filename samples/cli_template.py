

def get_options():

    parser 

    ...

def main():

    args = get_options()

    fn = args.fn
    
    config = args.config

    args, kwargs = parse_config(config)
    import fn

    fn(*args.args, **args.kwargs)

