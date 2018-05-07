# -*- coding: utf-8 -*-
from __future__ import absolute_import

import unittest

from google.cloud.bigquery import SchemaField

from bqdm.model.schema import BigQuerySchemaField


class TestBigQuerySchemaField(unittest.TestCase):

    def test_eq(self):
        schema_field1_1 = BigQuerySchemaField(
            'test',
            'STRING',
            'NULLABLE',
            'test_description',
            None
        )
        schema_field1_2 = BigQuerySchemaField(
            'test',
            'STRING',
            'NULLABLE',
            'test_description',
            None
        )
        self.assertEqual(schema_field1_1, schema_field1_2)

        schema_field2_1 = BigQuerySchemaField(
            'test',
            'INTEGER',
            'NULLABLE',
            'test_description',
            None
        )
        schema_field2_2 = BigQuerySchemaField(
            'test',
            'STRING',
            'REQUIRED',
            'foo_bar',
            None
        )
        self.assertNotEqual(schema_field2_1, schema_field2_2)

        schema_field3_1 = BigQuerySchemaField(
            'test',
            'RECORD',
            'NULLABLE',
            'test_description',
            (schema_field1_1, )
        )
        schema_field3_2 = BigQuerySchemaField(
            'test',
            'RECORD',
            'NULLABLE',
            'test_description',
            (schema_field1_1, )
        )
        schema_field3_3 = BigQuerySchemaField(
            'test',
            'RECORD',
            'NULLABLE',
            'test_description',
            [schema_field1_1]
        )
        self.assertEqual(schema_field3_1, schema_field3_2)
        self.assertEqual(schema_field3_1, schema_field3_3)

        schema_field4_1 = BigQuerySchemaField(
            'test',
            'RECORD',
            'NULLABLE',
            'test_description',
            (schema_field1_1)
        )
        schema_field4_2 = BigQuerySchemaField(
            'test',
            'RECORD',
            'NULLABLE',
            'test_description',
            (schema_field1_1, schema_field2_1)
        )
        schema_field4_3 = BigQuerySchemaField(
            'test',
            'RECORD',
            'NULLABLE',
            'test_description',
            (schema_field2_1, schema_field1_1)
        )
        schema_field4_4 = BigQuerySchemaField(
            'test',
            'RECORD',
            'NULLABLE',
            'test_description',
            [schema_field2_1, schema_field1_1]
        )
        self.assertNotEqual(schema_field4_1, schema_field4_2)
        self.assertEqual(schema_field4_2, schema_field4_3)
        self.assertNotEqual(schema_field4_2, schema_field4_4)

    def test_from_dict(self):
        expected_schema_field1 = BigQuerySchemaField(
            'test',
            'STRING',
            'NULLABLE',
            'test_description',
            None
        )
        actual_schema_field1_1 = BigQuerySchemaField.from_dict({
            'name': 'test',
            'field_type': 'STRING',
            'mode': 'NULLABLE',
            'description': 'test_description',
            'fields': None
        })
        self.assertEqual(expected_schema_field1, actual_schema_field1_1)
        actual_schema_field1_2 = BigQuerySchemaField.from_dict({
            'name': 'test',
            'field_type': 'INTEGER',
            'mode': 'REQUIRED',
            'description': 'foo_bar',
            'fields': None
        })
        self.assertNotEqual(expected_schema_field1, actual_schema_field1_2)

        expected_schema_field2 = BigQuerySchemaField(
            'test',
            'RECORD',
            'NULLABLE',
            'test_description',
            (expected_schema_field1, )
        )
        actual_schema_field2_1 = BigQuerySchemaField.from_dict({
            'name': 'test',
            'field_type': 'RECORD',
            'mode': 'NULLABLE',
            'description': 'test_description',
            'fields': [actual_schema_field1_1]
        })
        self.assertEqual(expected_schema_field2, actual_schema_field2_1)
        actual_schema_field2_2 = BigQuerySchemaField.from_dict({
            'name': 'test',
            'field_type': 'RECORD',
            'mode': 'NULLABLE',
            'description': 'test_description',
            'fields': [actual_schema_field1_2]
        })
        self.assertNotEqual(expected_schema_field2, actual_schema_field2_2)

    def test_from_schema_field(self):
        expected_schema_field1 = BigQuerySchemaField(
            'test',
            'STRING',
            'NULLABLE',
            'test_description',
            None
        )
        schema_field1_1 = SchemaField('test', 'STRING', 'NULLABLE', 'test_description')
        actual_schema_field1_1 = BigQuerySchemaField.from_schema_field(schema_field1_1)
        self.assertEqual(expected_schema_field1, actual_schema_field1_1)
        schema_field1_2 = SchemaField('test', 'INTEGER', 'REQUIRED', 'foo_bar')
        actual_schema_field1_2 = BigQuerySchemaField.from_schema_field(schema_field1_2)
        self.assertNotEqual(expected_schema_field1, actual_schema_field1_2)

        expected_schema_field2 = BigQuerySchemaField(
            'test',
            'RECORD',
            'NULLABLE',
            'test_description',
            (expected_schema_field1, )
        )
        schema_field2_1 = SchemaField('test', 'RECORD', 'NULLABLE', 'test_description',
                                      (schema_field1_1, ))
        actual_schema_field2_1 = BigQuerySchemaField.from_schema_field(schema_field2_1)
        self.assertEqual(expected_schema_field2, actual_schema_field2_1)
        schema_field2_2 = SchemaField('test', 'RECORD', 'NULLABLE', 'test_description',
                                      (schema_field1_2, ))
        actual_schema_field2_2 = BigQuerySchemaField.from_schema_field(schema_field2_2)
        self.assertNotEqual(expected_schema_field2, actual_schema_field2_2)

    def test_to_schema_field(self):
        expected_schema_field1 = SchemaField(
            'test',
            'STRING',
            'NULLABLE',
            'test_description'
        )
        schema_field1_1 = BigQuerySchemaField('test', 'STRING', 'NULLABLE', 'test_description')
        actual_schema_field1_1 = BigQuerySchemaField.to_schema_field(schema_field1_1)
        self.assertEqual(expected_schema_field1, actual_schema_field1_1)
        schema_field1_2 = BigQuerySchemaField('test', 'INTEGER', 'REQUIRED', 'foo_bar')
        actual_schema_field1_2 = BigQuerySchemaField.to_schema_field(schema_field1_2)
        self.assertNotEqual(expected_schema_field1, actual_schema_field1_2)

        expected_schema_field2 = SchemaField(
            'test',
            'RECORD',
            'NULLABLE',
            'test_description',
            (expected_schema_field1, )
        )
        schema_field2_1 = BigQuerySchemaField('test', 'RECORD', 'NULLABLE', 'test_description',
                                              (schema_field1_1, ))
        actual_schema_field2_1 = BigQuerySchemaField.to_schema_field(schema_field2_1)
        self.assertEqual(expected_schema_field2, actual_schema_field2_1)
        schema_field2_2 = BigQuerySchemaField('test', 'RECORD', 'NULLABLE', 'test_description',
                                              (schema_field1_2, ))
        actual_schema_field2_2 = BigQuerySchemaField.to_schema_field(schema_field2_2)
        self.assertNotEqual(expected_schema_field2, actual_schema_field2_2)

    def test_normalize_field_type(self):
        self.assertEqual('INT64', BigQuerySchemaField.normalize_field_type('INTEGER'))
        self.assertEqual('INT64', BigQuerySchemaField.normalize_field_type('INT64'))
        self.assertEqual('FLOAT64', BigQuerySchemaField.normalize_field_type('FLOAT'))
        self.assertEqual('FLOAT64', BigQuerySchemaField.normalize_field_type('FLOAT64'))
        self.assertEqual('BOOL', BigQuerySchemaField.normalize_field_type('BOOLEAN'))
        self.assertEqual('BOOL', BigQuerySchemaField.normalize_field_type('BOOL'))
        self.assertEqual('BYTES', BigQuerySchemaField.normalize_field_type('BYTES'))
        self.assertEqual('DATE', BigQuerySchemaField.normalize_field_type('DATE'))
        self.assertEqual('DATETIME', BigQuerySchemaField.normalize_field_type('DATETIME'))
        self.assertEqual('TIME', BigQuerySchemaField.normalize_field_type('TIME'))
        self.assertEqual('TIMESTAMP', BigQuerySchemaField.normalize_field_type('TIMESTAMP'))
        self.assertEqual('STRING', BigQuerySchemaField.normalize_field_type('STRING'))
        self.assertRaises(ValueError, BigQuerySchemaField.normalize_field_type('DECIMAL'))
