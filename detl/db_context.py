from contextlib import contextmanager

class DbStack():

    def __init__(self):

        self.stack = []

    def get_db(self):
        # TODO : get default db from the config folder
        return self.stack[-1] if len(self.stack) > 0 else None

    @contextmanager
    def get_controller(self, default):
        """A context manager for manipulating a stack."""
        try:
            self.stack.append(default)
            yield default
        finally:
            if self.stack:
                if self.stack[-1] is not default:
                    raise AssertionError(
                    "Nesting violated for default stack of %s objects" %
                    type(default))
                    self.stack.pop()
                else:
                    self.stack.remove(default)

db_context = DbStack()
