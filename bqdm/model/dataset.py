# -*- coding: utf-8 -*-
from __future__ import absolute_import

from collections import OrderedDict

from google.cloud.bigquery import DatasetReference
from google.cloud.bigquery.dataset import AccessEntry, Dataset


class BigQueryAccessEntry(object):

    def __init__(self, role, entity_type, entity_id):
        self.role = role
        self.entity_type = entity_type
        self.entity_id = entity_id

    @staticmethod
    def from_dict(value):
        return BigQueryAccessEntry(
            value.get('role', None),
            value.get('entity_type', None),
            value.get('entity_id', None),)

    @staticmethod
    def from_access_entry(value):
        return BigQueryAccessEntry(
            value.role,
            value.entity_type,
            value.entity_id,)

    @staticmethod
    def to_access_entry(model):
        return AccessEntry(model.role, model.entity_type, model.entity_id)

    @staticmethod
    def represent(dumper, data):
        return dumper.represent_mapping(
            'tag:yaml.org,2002:map',
            (
                ('role', data.role),
                ('entity_type', data.entity_type),
                ('entity_id', data.entity_id),
            )
        )

    def _key(self):
        return (self.role,
                self.entity_type,
                frozenset(self.entity_id.items())
                if isinstance(self.entity_id, (dict, OrderedDict,)) else self.entity_id,)

    def __eq__(self, other):
        if not isinstance(other, BigQueryAccessEntry):
            return NotImplemented
        return self._key() == other._key()

    def __ne__(self, other):
        return not self == other

    def __hash__(self):
        return hash(self._key())

    def __repr__(self):
        return 'BigQueryAccessEntry{0}'.format(self._key())


class BigQueryDataset(object):

    def __init__(self, dataset_id, friendly_name=None, description=None,
                 default_table_expiration_ms=None, location=None,
                 access_entries=None, labels=None):
        self.dataset_id = dataset_id
        self.friendly_name = friendly_name
        self.description = description
        self.default_table_expiration_ms = default_table_expiration_ms
        self.location = location
        self.access_entries = tuple(access_entries) if access_entries else None
        self.labels = labels if labels else None

    @staticmethod
    def from_dict(value):
        access_entries = value.get('access_entries', None)
        if access_entries:
            access_entries = tuple(BigQueryAccessEntry.from_dict(a)
                                   for a in access_entries)
        return BigQueryDataset(
            dataset_id=value.get('dataset_id', None),
            friendly_name=value.get('friendly_name', None),
            description=value.get('description', None),
            default_table_expiration_ms=value.get('default_table_expiration_ms', None),
            location=value.get('location', None),
            access_entries=access_entries,
            labels=value.get('labels', None))

    @staticmethod
    def from_dataset(dataset):
        access_entries = tuple(BigQueryAccessEntry.from_access_entry(a)
                               for a in dataset.access_entries) if dataset.access_entries else None
        return BigQueryDataset(
            dataset_id=dataset.dataset_id,
            friendly_name=dataset.friendly_name,
            description=dataset.description,
            default_table_expiration_ms=dataset.default_table_expiration_ms,
            location=dataset.location,
            access_entries=access_entries,
            labels=dataset.labels)

    @staticmethod
    def to_dataset(project, model):
        access_entries = model.access_entries
        if access_entries:
            access_entries = tuple(BigQueryAccessEntry.to_access_entry(a)
                                   for a in access_entries)
        else:
            access_entries = ()
        dataset_ref = DatasetReference(project, model.dataset_id)
        dataset = Dataset(dataset_ref)
        dataset.friendly_name = model.friendly_name
        dataset.description = model.description
        dataset.default_table_expiration_ms = model.default_table_expiration_ms
        dataset.location = model.location
        dataset.access_entries = access_entries
        dataset.labels = model.labels if model.labels is not None else dict()
        return dataset

    @staticmethod
    def represent(dumper, data):
        return dumper.represent_mapping(
            'tag:yaml.org,2002:map',
            (
                ('dataset_id', data.dataset_id),
                ('friendly_name', data.friendly_name),
                ('description', data.description),
                ('default_table_expiration_ms', data.default_table_expiration_ms),
                ('location', data.location),
                ('access_entries', data.access_entries),
                ('labels', data.labels),
            )
        )

    def _key(self):
        return (self.dataset_id,
                self.friendly_name,
                self.description,
                self.default_table_expiration_ms,
                self.location,
                frozenset(self.access_entries) if self.access_entries is not None
                else self.access_entries,
                frozenset(sorted(self.labels.items())) if self.labels is not None
                else self.labels,)

    def __eq__(self, other):
        if not isinstance(other, BigQueryDataset):
            return NotImplemented
        return self._key() == other._key()

    def __ne__(self, other):
        return not self == other

    def __hash__(self):
        return hash(self._key())

    def __repr__(self):
        return 'BigQueryDataset{0}'.format(self._key())
