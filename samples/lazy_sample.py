

with db.as_default(lazy=True):

    no_load = multiply(x,y)

final = statistics(no_load)
