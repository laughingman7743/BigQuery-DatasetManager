# -*- coding: utf-8 -*-
from __future__ import absolute_import

from google.cloud.bigquery.table import Table

from bqdm.model.schema import BigQuerySchemaField


class BigQueryTable(object):

    def __init__(self, table_id, friendly_name, description, expires, location,
                 partitioning_type, view_use_legacy_sql, view_query, schema,
                 labels):
        # TODO encryption_configuration
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
