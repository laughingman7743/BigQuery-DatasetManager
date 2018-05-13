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


def echo_dump(data):
    for line in dump(data).splitlines():
        click.echo('    {0}'.format(line))


def list_local_datasets(conf_dir):
    from bqdm.model.dataset import BigQueryDataset

    if not os.path.exists(conf_dir):
        raise RuntimeError('Configuration file directory not found.')

    datasets = []
    confs = glob.glob(os.path.join(conf_dir, '*.yml'))
    for conf in confs:
        click.echo('Load dataset config: {0}'.format(conf))
        with codecs.open(conf, 'rb', 'utf-8') as f:
            datasets.append(BigQueryDataset.from_dict(yaml.load(f)))
    if datasets:
        click.echo('------------------------------------------------------------------------')
        click.echo()
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
        click.echo('Load table config: {0}'.format(conf))
        with codecs.open(conf, 'rb', 'utf-8') as f:
            tables.append(BigQueryTable.from_dict(yaml.load(f)))
    if tables:
        click.echo('------------------------------------------------------------------------')
        click.echo()
    return tables


def ndiff(source, target):
    return difflib.ndiff(dump(source).splitlines(),
                         dump(target).splitlines())


def echo_ndiff(source, target, fg='yellow'):
    for d in ndiff(source, target):
        click.secho('    {0}'.format(d), fg=fg)


def parse_expires(value):
    return parse(value)
