# -*- coding: utf-8 -*-
from __future__ import absolute_import
from collections import OrderedDict

from google.cloud.bigquery.dataset import AccessEntry, Dataset
from google.cloud.bigquery.schema import SchemaField
from google.cloud.bigquery.table import Table


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


class BigQueryTable(object):

    def __init__(self, table_id, friendly_name, description, expires, location,
                 partitioning_type, view_use_legacy_sql, view_query, schema,
                 labels):
        # TODO external_data_configuration
        self.table_id = table_id
        self.friendly_name = friendly_name
        self.description = description
        self.expires = expires
        self.location = location
        self.partitioning_type = partitioning_type
        self.view_use_legacy_sql = view_use_legacy_sql
        self.view_query = view_query
        self.schema = schema if schema else None
        self.labels = labels if labels else None

    @staticmethod
    def from_dict(value):
        schema = value.get('schema', None)
        if schema:
            schema = [BigQuerySchemaField.from_dict(s) for s in schema]
        return BigQueryTable(
            value.get('table_id', None),
            value.get('friendly_name', None),
            value.get('description', None),
            value.get('expires', None),
            value.get('location', None),
            value.get('partitioning_type', None),
            value.get('view_use_legacy_sql', None),
            value.get('view_query', None),
            schema,
            value.get('labels', None),)

    @staticmethod
    def from_table(table):
        schema = table.schema
        if schema:
            schema = [BigQuerySchemaField.from_schema_field(s) for s in schema]
        return BigQueryTable(
            table.table_id,
            table.friendly_name,
            table.description,
            table.expires,
            table.location,
            table.partitioning_type,
            table.view_use_legacy_sql,
            table.view_query,
            schema,
            table.labels,)

    @staticmethod
    def to_table(dataset, model):
        schema = model.schema
        if schema:
            schema = [BigQuerySchemaField.to_schema_field(s) for s in schema]
        else:
            schema = ()
        table_ref = dataset.table(model.table_id)
        table = Table(table_ref, schema)
        table.friendly_name = model.friendly_name
        table.description = model.description
        table.expires = model.expires
        table.location = model.location
        table.partitioning_type = model.partitioning_type
        if model.view_use_legacy_sql is not None:
            table.view_use_legacy_sql = model.view_use_legacy_sql
        if model.view_query is not None:
            table.view_query = model.view_query
        table.labels = model.labels if model.labels is not None else dict()
        return table

    @staticmethod
    def represent(dumper, data):
        return dumper.represent_mapping(
            'tag:yaml.org,2002:map',
            (
                ('table_id', data.table_id),
                ('friendly_name', data.friendly_name),
                ('description', data.description),
                ('expires', data.expires),
                ('location', data.location),
                ('partitioning_type', data.partitioning_type),
                ('view_use_legacy_sql', data.view_use_legacy_sql),
                ('view_query', data.view_query),
                ('schema', data.schema),
                ('labels', data.labels),
            )
        )

    def _key(self):
        return (self.table_id,
                self.friendly_name,
                self.description,
                self.expires,
                self.location,
                self.partitioning_type,
                self.view_use_legacy_sql,
                self.view_query,
                frozenset(self.schema) if self.schema is not None
                else self.schema,
                frozenset(sorted(self.labels.items())) if self.labels is not None
                else self.labels,)

    def __eq__(self, other):
        if not isinstance(other, BigQueryTable):
            return NotImplemented
        return self._key() == other._key()

    def __ne__(self, other):
        return not self == other

    def __hash__(self):
        return hash(self._key())

    def __repr__(self):
        return 'BigQueryTable{0}'.format(self._key())


class BigQuerySchemaField(object):

    def __init__(self, name, field_type, mode, description, fields):
        self.name = name
        self.field_type = field_type
        self.mode = mode
        self.description = description
        self.fields = fields if fields else None

    @staticmethod
    def from_dict(value):
        fields = value.get('fields', None)
        if fields:
            fields = [BigQuerySchemaField.from_dict(f) for f in fields]
        return BigQuerySchemaField(
            value.get('name', None),
            value.get('field_type', None),
            value.get('mode', None),
            value.get('description', None),
            fields,)

    @staticmethod
    def from_schema_field(schema_field):
        fields = schema_field.fields
        if fields:
            fields = [BigQuerySchemaField.from_schema_field(f) for f in fields]
        return BigQuerySchemaField(
            schema_field.name,
            schema_field.field_type,
            schema_field.mode,
            schema_field.description,
            fields,)

    @staticmethod
    def to_schema_field(model):
        fields = model.fields
        if fields:
            fields = [BigQuerySchemaField.to_schema_field(f) for f in fields]
        else:
            fields = ()
        return SchemaField(model.name, model.field_type, model.mode,
                           model.description, fields)

    @staticmethod
    def represent(dumper, data):
        return dumper.represent_mapping(
            'tag:yaml.org,2002:map',
            (
                ('name', data.name),
                ('field_type', data.field_type),
                ('mode', data.mode),
                ('description', data.description),
                ('fields', data.fields),
            )
        )

    @staticmethod
    def normalize_field_type(field_type):
        field_type = field_type.upper()
        if field_type in ['INTEGER', 'INT64']:
            return 'INT64'
        elif field_type in ['FLOAT', 'FLOAT64']:
            return 'FLOAT64'
        elif field_type in ['BOOLEAN', 'BOOL']:
            return 'BOOL'
        elif field_type in ['BYTES']:
            return 'BYTES'
        elif field_type in ['DATE']:
            return 'DATE'
        elif field_type in ['DATETIME']:
            return 'DATETIME'
        elif field_type in ['TIME']:
            return 'TIME'
        elif field_type in ['TIMESTAMP']:
            return 'TIMESTAMP'
        elif field_type in ['STRING']:
            return 'STRING'
        else:
            raise RuntimeError('Unknown field type.')

    def _key(self):
        return (self.name,
                self.field_type,
                self.mode,
                self.description,
                frozenset(self.fields) if self.fields is not None
                else self.fields,)

    def __eq__(self, other):
        if not isinstance(other, BigQuerySchemaField):
            return NotImplemented
        return self._key() == other._key()

    def __ne__(self, other):
        return not self == other

    def __hash__(self):
        return hash(self._key())

    def __repr__(self):
        return 'BigQuerySchemaField{0}'.format(self._key())
