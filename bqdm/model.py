# -*- coding: utf-8 -*-
from __future__ import absolute_import
from collections import OrderedDict

from future.utils import iteritems
from google.cloud.bigquery.dataset import Dataset, AccessGrant


class BigQueryDataset(object):

    def __init__(self, name, friendly_name, description,
                 default_table_expiration_ms, location, access_grants):
        self.name = name
        self.friendly_name = friendly_name
        self.description = description
        self.default_table_expiration_ms = default_table_expiration_ms
        self.location = location
        self.access_grants = access_grants

    @staticmethod
    def from_dict(value):
        access_grants = value.get('access_grants', None)
        if access_grants:
            access_grants = [BigQueryAccessGrant.from_dict(a) for a in access_grants]
        return BigQueryDataset(value.get('name', None),
                               value.get('friendly_name', None),
                               value.get('description', None),
                               value.get('default_table_expiration_ms', None),
                               value.get('location', None),
                               access_grants)

    @staticmethod
    def from_dataset(value):
        value.reload()
        access_grants = value.access_grants
        if access_grants:
            access_grants = [BigQueryAccessGrant.from_access_grant(a) for a in access_grants]
        return BigQueryDataset(value.name,
                               value.friendly_name,
                               value.description,
                               value.default_table_expiration_ms,
                               value.location,
                               access_grants)

    @staticmethod
    def to_dataset(client, value):
        access_grants = value.access_grants
        if access_grants:
            access_grants = tuple([BigQueryAccessGrant.to_access_grant(a) for a in access_grants])
        dataset = Dataset(value.name, client, access_grants)
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
                ('access_grants', value.access_grants),
            )
        )

    def _key(self):
        return (self.name,
                self.friendly_name,
                self.description,
                self.default_table_expiration_ms,
                self.location,
                frozenset(self.access_grants) if self.access_grants is not None
                else self.access_grants,)

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


class BigQueryAccessGrant(object):

    def __init__(self, role, entity_type, entity_id):
        self.role = role
        self.entity_type = entity_type
        self.entity_id = entity_id

    @staticmethod
    def from_dict(value):
        return BigQueryAccessGrant(
            value.get('role', None),
            value.get('entity_type', None),
            value.get('entity_id', None),)

    @staticmethod
    def from_access_grant(value):
        return BigQueryAccessGrant(
            value.role,
            value.entity_type,
            value.entity_id,)

    @staticmethod
    def to_access_grant(value):
        return AccessGrant(value.role, value.entity_type, value.entity_id)

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
        if not isinstance(other, BigQueryAccessGrant):
            return NotImplemented
        return self._key() == other._key()

    def __ne__(self, other):
        return not self == other

    def __hash__(self):
        return hash(self._key())

    def __repr__(self):
        return 'BigQueryAccessGrant{0}'.format(self._key())
