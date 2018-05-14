# -*- coding: utf-8 -*-
from __future__ import absolute_import

import codecs
import difflib
import functools
import glob
import os
import threading
from concurrent import futures

import click
import yaml
from dateutil.parser import parse

try:
    from multiprocessing import cpu_count
except ImportError:
    def cpu_count():
        return None


def get_parallelism():
    return (cpu_count() or 1) * 5


def as_completed(fs):
    return (f.result() for f in futures.as_completed(fs))


def str_representer(dumper, data):
    if '\n' in data:
        return dumper.represent_scalar('tag:yaml.org,2002:str', data, style='|')
    else:
        return dumper.represent_scalar('tag:yaml.org,2002:str', data)


def tuple_representer(dumper, data):
    return dumper.represent_sequence('tag:yaml.org,2002:seq', data)


def synchronized(wrapped):
    """The missing @synchronized decorator

    https://git.io/vydTA"""
    _lock = threading.RLock()

    @functools.wraps(wrapped)
    def _wrapper(*args, **kwargs):
        with _lock:
            return wrapped(*args, **kwargs)
    return _wrapper


def dump(data):
    return yaml.dump(data, default_flow_style=False, indent=4,
                     allow_unicode=True, canonical=False)


def load_dataset(conf):
    from bqdm.model.dataset import BigQueryDataset

    echo('Load dataset config: {0}'.format(conf))
    with codecs.open(conf, 'rb', 'utf-8') as f:
        return BigQueryDataset.from_dict(yaml.load(f))


def list_local_datasets(conf_dir, include_datasets=(), exclude_datasets=()):
    if not os.path.exists(conf_dir):
        raise RuntimeError('Configuration file directory not found.')

    datasets = []
    confs = glob.glob(os.path.join(conf_dir, '*.yml'))
    for conf in confs:
        dataset_id = os.path.splitext(os.path.basename(conf))[0]
        if not include_datasets:
            if dataset_id not in exclude_datasets:
                datasets.append(load_dataset(conf))
        else:
            if dataset_id in include_datasets and dataset_id not in exclude_datasets:
                datasets.append(load_dataset(conf))
    if datasets:
        echo('------------------------------------------------------------------------')
        echo()
    return datasets


def load_table(conf):
    from bqdm.model.table import BigQueryTable

    echo('Load table config: {0}'.format(conf))
    with codecs.open(conf, 'rb', 'utf-8') as f:
        return BigQueryTable.from_dict(yaml.load(f))


def list_local_tables(conf_dir, dataset_id):
    conf_dir = os.path.join(conf_dir, dataset_id)
    if not os.path.exists(conf_dir):
        # table resources unmanaged
        return None

    tables = []
    confs = glob.glob(os.path.join(conf_dir, '*.yml'))
    for conf in confs:
        tables.append(load_table(conf))
    if tables:
        echo('------------------------------------------------------------------------')
        echo()
    return tables


def ndiff(source, target):
    return difflib.ndiff(dump(source).splitlines(),
                         dump(target).splitlines())


def parse_expires(value):
    return parse(value)


def _echo(text=None, prefix='', fg=None, no_color=False):
    if not text:
        click.echo()
    elif no_color:
        click.echo('{prefix}{text}'.format(prefix=prefix, text=text))
    else:
        click.secho('{prefix}{text}'.format(prefix=prefix, text=text), fg=fg)


@synchronized
def echo(text=None, prefix='', fg=None, no_color=False):
    _echo(text=text, prefix=prefix, fg=fg, no_color=no_color)


@synchronized
def echo_dump(data, prefix='    ', fg=None, no_color=False):
    for line in dump(data).splitlines():
        _echo(line, prefix=prefix, fg=fg, no_color=no_color)


@synchronized
def echo_ndiff(source, target, prefix='    ', fg='yellow', no_color=False):
    diff = ndiff(source, target)
    for d in diff:
        _echo(d, prefix=prefix, fg=fg, no_color=no_color)
    if diff:
        _echo()
