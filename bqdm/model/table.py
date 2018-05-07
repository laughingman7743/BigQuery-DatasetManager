# -*- coding: utf-8 -*-
from __future__ import absolute_import

from google.cloud.bigquery import TableReference
from google.cloud.bigquery.table import Table

from bqdm.model.schema import BigQuerySchemaField


class BigQueryTable(object):

    def __init__(self, table_id, friendly_name=None, description=None,
                 expires=None, location=None, partitioning_type=None,
                 view_use_legacy_sql=None, view_query=None, schema=None,
                 labels=None):
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
        self.schema = tuple(schema) if schema else None
        self.labels = labels if labels else None

    @staticmethod
    def from_dict(value):
        schema = value.get('schema', None)
        if schema:
            schema = tuple(BigQuerySchemaField.from_dict(s) for s in schema)
        return BigQueryTable(
            table_id=value.get('table_id', None),
            friendly_name=value.get('friendly_name', None),
            description=value.get('description', None),
            expires=value.get('expires', None),
            location=value.get('location', None),
            partitioning_type=value.get('partitioning_type', None),
            view_use_legacy_sql=value.get('view_use_legacy_sql', None),
            view_query=value.get('view_query', None),
            schema=schema,
            labels=value.get('labels', None),)

    @staticmethod
    def from_table(table):
        schema = tuple(BigQuerySchemaField.from_schema_field(s)
                       for s in table.schema) if table.schema else None
        return BigQueryTable(
            table_id=table.table_id,
            friendly_name=table.friendly_name,
            description=table.description,
            expires=table.expires,
            location=table.location,
            partitioning_type=table.partitioning_type,
            view_use_legacy_sql=table.view_use_legacy_sql,
            view_query=table.view_query,
            schema=schema,
            labels=table.labels)

    @staticmethod
    def to_table(dataset_ref, model):
        schema = model.schema
        if schema:
            schema = tuple(BigQuerySchemaField.to_schema_field(s) for s in schema)
        else:
            schema = ()
        table_ref = TableReference(dataset_ref, model.table_id)
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
