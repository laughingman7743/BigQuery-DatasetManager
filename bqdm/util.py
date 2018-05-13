# -*- coding: utf-8 -*-
from __future__ import absolute_import

import codecs
import difflib
import glob
import os
from dateutil.parser import parse

import click
import yaml


def str_representer(dumper, data):
    if '\n' in data:
        return dumper.represent_scalar('tag:yaml.org,2002:str', data, style='|')
    else:
        return dumper.represent_scalar('tag:yaml.org,2002:str', data)


def tuple_representer(dumper, data):
    return dumper.represent_sequence('tag:yaml.org,2002:seq', data)


def dump(data):
    return yaml.dump(data, default_flow_style=False, indent=4,
                     allow_unicode=True, canonical=False)


def list_local_datasets(conf_dir):
    from bqdm.model.dataset import BigQueryDataset

    if not os.path.exists(conf_dir):
        raise RuntimeError('Configuration file directory not found.')

    datasets = []
    confs = glob.glob(os.path.join(conf_dir, '*.yml'))
    for conf in confs:
        echo('Load dataset config: {0}'.format(conf))
        with codecs.open(conf, 'rb', 'utf-8') as f:
            datasets.append(BigQueryDataset.from_dict(yaml.load(f)))
    if datasets:
        echo('------------------------------------------------------------------------')
        echo()
    return datasets


def list_local_tables(conf_dir, dataset_id):
    from bqdm.model.table import BigQueryTable

    conf_dir = os.path.join(conf_dir, dataset_id)
    if not os.path.exists(conf_dir):
        # table resources unmanaged
        return None

    tables = []
    confs = glob.glob(os.path.join(conf_dir, '*.yml'))
    for conf in confs:
        echo('Load table config: {0}'.format(conf))
        with codecs.open(conf, 'rb', 'utf-8') as f:
            tables.append(BigQueryTable.from_dict(yaml.load(f)))
    if tables:
        echo('------------------------------------------------------------------------')
        echo()
    return tables


def ndiff(source, target):
    return difflib.ndiff(dump(source).splitlines(),
                         dump(target).splitlines())


def parse_expires(value):
    return parse(value)


def echo(text=None, prefix='', fg=None, no_color=False):
    if not text:
        click.echo()
    elif no_color:
        click.echo('{prefix}{text}'.format(prefix=prefix, text=text))
    else:
        click.secho('{prefix}{text}'.format(prefix=prefix, text=text), fg=fg)


def echo_dump(data, prefix='    ', fg=None, no_color=False):
    for line in dump(data).splitlines():
        echo(line, prefix=prefix, fg=fg, no_color=no_color)


def echo_ndiff(source, target, prefix='    ', fg='yellow', no_color=False):
    diff = ndiff(source, target)
    for d in diff:
        echo(d, prefix=prefix, fg=fg, no_color=no_color)
    if diff:
        echo()
