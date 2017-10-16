# -*- coding: utf-8 -*-
from __future__ import absolute_import
import codecs
import difflib
import glob
import os
from collections import OrderedDict

import click
import yaml
from bqdm.model import BigQueryDataset


def str_representer(_, data):
    return yaml.ScalarNode('tag:yaml.org,2002:str', data)


def ordered_dict_constructor(loader, data):
    return OrderedDict(loader.construct_pairs(data))


def dump_dataset(data):
    return yaml.dump(data, default_flow_style=False, indent=4,
                     allow_unicode=True, canonical=False)


def load_local_datasets(conf_dir):
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


def load_bigquery_datasets(client):
    datasets = []
    for dataset in client.list_datasets():
        click.echo('Load: ' + dataset.path)
        datasets.append(BigQueryDataset.from_dataset(dataset))
    click.echo('------------------------------------------------------------------------')
    click.echo()
    return datasets


def ndiff(source, target):
    return difflib.ndiff(dump_dataset(source).splitlines(),
                         dump_dataset(target).splitlines())


def get_add_datasets(source, target):
    names = set(t.name for t in target) - set(s.name for s in source)
    results = [t for t in target if t.name in names]
    return len(results), tuple(results)


def get_change_datasets(source, target):
    _, add_datasets = get_add_datasets(source, target)
    results = (set(target) - set(add_datasets)) - set(source)
    return len(results), tuple(results)


def get_destroy_datasets(source, target):
    names = set(s.name for s in source) - set(t.name for t in target)
    results = [s for s in source if s.name in names]
    return len(results), tuple(results)


def get_intersection_datasets(source, target):
    names = set(s.name for s in source) & set(t.name for t in target)
    results = [s for s in source if s.name in names]
    return len(results), tuple(results)
