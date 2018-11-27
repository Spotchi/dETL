import click
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from click import UsageError
from ck.date_utils import get_date_from_str
from ck.builders import get_store_from_uri

local_store_dir = os.environ['LOCAL_DETL_STORE']

@click.group()
@click.version_option()
def cli():
    pass


@cli.command()
@click.option("--local_store", metavar="PATH", default=local_store_dir)
@click.argument("remote_store")
@click.argument("namespace")
@click.option("-a", "--after", default=None)
@click.option("-b", "--before", default=None)
@click.option("-t", "--tag", multiple=True)
def diff(store, namespace, after, before, tags):
    m_store = get_store_from_uri(store)
    after = get_date_from_str(after)
    before = get_date_from_str(before)
    matches = m_store.get_results_namespace(namespace, min_date=after, max_date=before, tags=tags)
    print(matches)


@cli.command()
@click.option("--local_store", metavar="PATH", default=local_store_dir)
@click.argument("remote_store")
@click.argument("namespace")
@click.option("-a", "--after", default=None)
@click.option("-b", "--before", default=None)
@click.option("-t", "--tag", multiple=True)
@click.option("-c", "--commit")
def pull(local_store, remote_store, namespace, after, before, tags, commit):
    l_store = get_store_from_uri(local_store)
    m_store = get_store_from_uri(remote_store)
    after = get_date_from_str(after)
    before = get_date_from_str(before)
    matches = m_store.get_results_namespace(namespace, min_date=after, max_date=before, tags=tags, commit=commit)
    m_store.pull(matches, l_store)


@cli.command()
@click.option("--local_store", metavar="PATH", default=local_store_dir)
@click.argument("remote_store")
@click.argument("namespace")
@click.option("-a", "--after", default=None)
@click.option("-b", "--before", default=None)
@click.option("-t", "--tag", multiple=True)
@click.option("-c", "--commit")
def push(local_store, remote_store, namespace, after, before, tags, commit):
    l_store = get_store_from_uri(local_store)
    m_store = get_store_from_uri(remote_store)
    after = get_date_from_str(after)
    before = get_date_from_str(before)
    matches = l_store.get_results_namespace(namespace, min_date=after, max_date=before, tags=tags, commit=commit)
    m_store.push(matches, l_store)


if __name__ == '__main__':
    cli()
