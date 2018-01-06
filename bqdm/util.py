# -*- coding: utf-8 -*-
from __future__ import absolute_import
import codecs
import difflib
import glob
import os

import click
import yaml
from bqdm.dataset import BigQueryDataset
from bqdm.table import BigQueryTable


def str_representer(dumper, data):
    if '\n' in data:
        return dumper.represent_scalar('tag:yaml.org,2002:str', data, style='|')
    else:
        return dumper.represent_scalar('tag:yaml.org,2002:str', data)


def dump(data):
    return yaml.dump(data, default_flow_style=False, indent=4,
                     allow_unicode=True, canonical=False)


def list_local_datasets(conf_dir):
    if not os.path.exists(conf_dir):
        raise RuntimeError('Configuration file directory not found.')

    datasets = []
    confs = glob.glob(os.path.join(conf_dir, '*.yml'))
    for conf in confs:
        click.echo('Load: {0}'.format(conf))
        with codecs.open(conf, 'rb', 'utf-8') as f:
            datasets.append(BigQueryDataset.from_dict(yaml.load(f)))
    click.echo('------------------------------------------------------------------------')
    click.echo()
    return datasets


def list_local_tables(conf_dir, dataset_id):
    conf_dir = os.path.join(conf_dir, dataset_id)
    if not os.path.exists(conf_dir):
        # table resources unmanaged
        return None

    tables = []
    confs = glob.glob(os.path.join(conf_dir, '*.yml'))
    for conf in confs:
        click.echo('Load: {0}'.format(conf))
        with codecs.open(conf, 'rb', 'utf-8') as f:
            tables.append(BigQueryTable.from_dict(yaml.load(f)))
    click.echo('------------------------------------------------------------------------')
    click.echo()
    return tables


def ndiff(source, target):
    return difflib.ndiff(dump(source).splitlines(),
                         dump(target).splitlines())
