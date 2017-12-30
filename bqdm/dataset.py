# -*- coding: utf-8 -*-
from __future__ import absolute_import
from collections import OrderedDict

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

    def __init__(self, dataset_id, friendly_name, description,
                 default_table_expiration_ms, location, labels, access_entries):
        self.dataset_id = dataset_id
        self.friendly_name = friendly_name
        self.description = description
        self.default_table_expiration_ms = default_table_expiration_ms
        self.location = location
        self.labels = labels if labels else None
        self.access_entries = access_entries if access_entries else None

    @staticmethod
    def from_dict(value):
        access_entries = value.get('access_entries', None)
        if access_entries:
            access_entries = [BigQueryAccessEntry.from_dict(a) for a in access_entries]
        return BigQueryDataset(value.get('dataset_id', None),
                               value.get('friendly_name', None),
                               value.get('description', None),
                               value.get('default_table_expiration_ms', None),
                               value.get('location', None),
                               value.get('labels', None),
                               access_entries)

    @staticmethod
    def from_dataset(dataset):
        access_entries = dataset.access_entries
        if access_entries:
            access_entries = [BigQueryAccessEntry.from_access_entry(a) for a in access_entries]
        return BigQueryDataset(dataset.dataset_id,
                               dataset.friendly_name,
                               dataset.description,
                               dataset.default_table_expiration_ms,
                               dataset.location,
                               dataset.labels,
                               access_entries)

    @staticmethod
    def to_dataset(client, model):
        access_entries = model.access_entries
        if access_entries:
            access_entries = [BigQueryAccessEntry.to_access_entry(a) for a in access_entries]
        else:
            access_entries = ()
        dataset_ref = client.dataset(model.dataset_id)
        dataset = Dataset(dataset_ref)
        dataset.friendly_name = model.friendly_name
        dataset.description = model.description
        dataset.default_table_expiration_ms = model.default_table_expiration_ms
        dataset.location = model.location
        dataset.labels = model.labels if model.labels is not None else dict()
        dataset.access_entries = access_entries
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
                ('labels', data.labels),
                ('access_entries', data.access_entries),
            )
        )

    def _key(self):
        return (self.dataset_id,
                self.friendly_name,
                self.description,
                self.default_table_expiration_ms,
                self.location,
                frozenset(sorted(self.labels.items())) if self.labels is not None
                else self.labels,
                frozenset(self.access_entries) if self.access_entries is not None
                else self.access_entries,)

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
