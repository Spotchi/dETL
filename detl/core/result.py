# Result is the step when the DAG has been linked together and identities have been established, but no results have been
# computed yet


class Result(object):

    def __init__(self, value, identity):
        # self._value = value
        self._identity = identity


    def create_run_info(run_uuid):
        pass
    # TODO : This will include commit and user as well

        run_id = Run(run_info, RunData(result_value))

        # TODO : will need different options but for now just try with one, add latest
