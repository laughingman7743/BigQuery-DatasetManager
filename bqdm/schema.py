# -*- coding: utf-8 -*-
from __future__ import absolute_import

from google.cloud.bigquery.schema import SchemaField


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
