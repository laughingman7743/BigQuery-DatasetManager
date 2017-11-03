# -*- coding: utf-8 -*-
from __future__ import absolute_import
from collections import OrderedDict

from future.utils import iteritems
from google.cloud.bigquery.dataset import Dataset, AccessEntry


class BigQueryDataset(object):

    def __init__(self, name, friendly_name, description,
                 default_table_expiration_ms, location, access_entries):
        self.name = name
        self.friendly_name = friendly_name
        self.description = description
        self.default_table_expiration_ms = default_table_expiration_ms
        self.location = location
        self.access_entries = access_entries

    @staticmethod
    def from_dict(value):
        access_entries = value.get('access_entries', None)
        if access_entries:
            access_entries = [BigQueryAccessEntry.from_dict(a) for a in access_entries]
        return BigQueryDataset(value.get('name', None),
                               value.get('friendly_name', None),
                               value.get('description', None),
                               value.get('default_table_expiration_ms', None),
                               value.get('location', None),
                               access_entries)

    @staticmethod
    def from_dataset(value):
        value.reload()
        access_entries = value.access_entries
        if access_entries:
            access_entries = [BigQueryAccessEntry.from_access_entry(a) for a in access_entries]
        return BigQueryDataset(value.name,
                               value.friendly_name,
                               value.description,
                               value.default_table_expiration_ms,
                               value.location,
                               access_entries)

    @staticmethod
    def to_dataset(client, value):
        access_entries = value.access_entries
        if access_entries:
            access_entries = tuple([BigQueryAccessEntry.to_access_entry(a) for a in access_entries])
        dataset = Dataset(value.name, client, access_entries)
        dataset.friendly_name = value.friendly_name
        dataset.description = value.description
        dataset.default_table_expiration_ms = value.default_table_expiration_ms
        dataset.location = value.location
        return dataset

    @staticmethod
    def represent(dumper, value):
        return dumper.represent_mapping(
            'tag:yaml.org,2002:map',
            (
                ('name', value.name),
                ('friendly_name', value.friendly_name),
                ('description', value.description),
                ('default_table_expiration_ms', value.default_table_expiration_ms),
                ('location', value.location),
                ('access_entries', value.access_entries),
            )
        )

    def _key(self):
        return (self.name,
                self.friendly_name,
                self.description,
                self.default_table_expiration_ms,
                self.location,
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
    def to_access_entry(value):
        return AccessEntry(value.role, value.entity_type, value.entity_id)

    @staticmethod
    def represent(dumper, value):
        return dumper.represent_mapping(
            'tag:yaml.org,2002:map',
            (
                ('role', value.role),
                ('entity_type', value.entity_type),
                ('entity_id', value.entity_id),
            )
        )

    def _key(self):
        return (self.role,
                self.entity_type,
                frozenset((k, v) for k, v in iteritems(self.entity_id))
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
