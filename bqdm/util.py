# -*- coding: utf-8 -*-
from __future__ import absolute_import
import codecs
import difflib
import glob
import os

import click
import yaml
from bqdm.model import BigQueryDataset


def str_representer(_, data):
    return yaml.ScalarNode('tag:yaml.org,2002:str', data)


def dump_dataset(data):
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


def ndiff(source, target):
    return difflib.ndiff(dump_dataset(source).splitlines(),
                         dump_dataset(target).splitlines())
