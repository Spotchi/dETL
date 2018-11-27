import click
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from click import UsageError
from ck.date_utils import get_date_from_str
from ck.builders import get_store_from_uri


@click.group()
@click.version_option()
def cli():
    pass


@cli.command()
@click.option("--store", metavar="PATH", default=None,
              help="The root of the backing file store for experiment and run data "
                   "(default: ./data).")
@click.argument("namespace")
@click.option("-a", "--after", default=None)
@click.option("-b", "--before", default=None)
@click.option("-t", "--tag", multiple=True)
def ls(store, namespace, after, before, tag):
    print("In browse")
    print(store, namespace, after, before, tag)
    m_store = get_store_from_uri(store)
    after = get_date_from_str(after)
    before = get_date_from_str(before)
    matches = m_store.get_results_namespace(namespace, min_date=after, max_date=before, tags=tag)
    print(matches)


if __name__ == "__main__":
    ls()
