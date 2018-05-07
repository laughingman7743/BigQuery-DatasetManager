# -*- coding: utf-8 -*-
from __future__ import absolute_import

import functools
import os

from google.cloud import bigquery
from google.cloud.bigquery import DatasetReference, Dataset, TableReference, Table


class Env(object):

    def __init__(self):
        self.credential = os.getenv('GOOGLE_APPLICATION_CREDENTIALS', None)
        assert self.credential, \
            'Required environment variable `GOOGLE_APPLICATION_CREDENTIALS` not found.'
        self.project = os.getenv('GOOGLE_CLOUD_PROJECT', None)
        assert self.project, \
            'Required environment variable `GOOGLE_CLOUD_PROJECT` not found.'


def with_client(fn):
    @functools.wraps(fn)
    def wrapped_fn(self, *args, **kwargs):
        client = bigquery.Client()
        fn(self, client, *args, **kwargs)
    return wrapped_fn


def make_dataset(project, dataset_id, friendly_name=None, description=None,
                 default_table_expiration_ms=None, location=None,
                 labels=None, access_entries=None):
    dataset_ref = DatasetReference(project, dataset_id)
    dataset = Dataset(dataset_ref)
    dataset.friendly_name = friendly_name
    dataset.description = description
    dataset.default_table_expiration_ms = default_table_expiration_ms
    dataset.location = location
    dataset.labels = labels
    dataset.access_entries = access_entries
    return dataset


def make_table(project, dataset_id, table_id, friendly_name=None, description=None,
               expires=None, location=None, partitioning_type=None, view_use_legacy_sql=None,
               view_query=None, schema=None, labels=None):
    dataset_ref = DatasetReference(project, dataset_id)
    table_ref = TableReference(dataset_ref, table_id)
    table = Table(table_ref)
    table.friendly_name = friendly_name
    table.description = description
    table.expires = expires
    table.location = location
    table.partitioning_type = partitioning_type
    if view_use_legacy_sql is not None:
        table.view_use_legacy_sql = view_use_legacy_sql
    table.view_query = view_query
    table.schema = schema
    table.labels = labels
    return table
