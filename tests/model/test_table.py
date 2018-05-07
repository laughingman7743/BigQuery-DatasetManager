# -*- coding: utf-8 -*-
from __future__ import absolute_import

import unittest
from datetime import datetime

from google.cloud.bigquery import DatasetReference, SchemaField
from pytz import UTC

from bqdm.model.schema import BigQuerySchemaField
from bqdm.model.table import BigQueryTable
from tests.util import make_table


class TestBigQueryTable(unittest.TestCase):

    def test_eq(self):
        table1_1 = BigQueryTable(
            table_id='test',
            friendly_name='test_friendly_name',
            description='test_description',
            location='US'
        )
        table1_2 = BigQueryTable(
            table_id='test',
            friendly_name='test_friendly_name',
            description='test_description',
            location='US'
        )
        self.assertEqual(table1_1, table1_2)

        table2_1 = BigQueryTable(
            table_id='test',
            friendly_name='test_friendly_name',
            description='test_description',
            location='US'
        )
        table2_2 = BigQueryTable(
            table_id='test',
            friendly_name='fizz_buzz',
            description='foo_bar',
            expires=datetime(2018, 1, 1, 0, 0, 0, tzinfo=UTC),
            location='EU',
            partitioning_type='DAY',
        )
        self.assertNotEqual(table2_1, table2_2)

        table3_1 = BigQueryTable(
            table_id='test',
            friendly_name='test_friendly_name',
            description='test_description',
            location='US'
        )
        table3_2 = BigQueryTable(
            table_id='test',
            friendly_name='test_friendly_name',
            description='test_description',
            view_use_legacy_sql=False,
            view_query='SELECT * FROM bigquery_datasetmanager.test.test'
        )
        self.assertNotEqual(table3_1, table3_2)

        schema_field1 = BigQuerySchemaField(
            name='test',
            field_type='INTEGER',
            mode='NULLABLE',
            description='test_description'
        )
        schema_field2 = BigQuerySchemaField(
            name='test',
            field_type='STRING',
            mode='REQUIRED',
            description='foo_bar'
        )

        table4_1 = BigQueryTable(
            table_id='test',
            friendly_name='test_friendly_name',
            description='test_description',
            location='US',
            schema=(schema_field1, )
        )
        table4_2 = BigQueryTable(
            table_id='test',
            friendly_name='test_friendly_name',
            description='test_description',
            location='US',
            schema=(schema_field1, )
        )
        self.assertEqual(table4_1, table4_2)

        table5_1 = BigQueryTable(
            table_id='test',
            friendly_name='test_friendly_name',
            description='test_description',
            location='US',
            schema=(schema_field1, )
        )
        table5_2 = BigQueryTable(
            table_id='test',
            friendly_name='test_friendly_name',
            description='test_description',
            location='US',
            schema=(schema_field2, )
        )
        self.assertNotEqual(table5_1, table5_2)

        table6_1 = BigQueryTable(
            table_id='test',
            friendly_name='test_friendly_name',
            description='test_description',
            location='US',
            schema=(schema_field1, schema_field2)
        )
        table6_2 = BigQueryTable(
            table_id='test',
            friendly_name='test_friendly_name',
            description='test_description',
            location='US',
            schema=(schema_field2, schema_field1)
        )
        table6_3 = BigQueryTable(
            table_id='test',
            friendly_name='test_friendly_name',
            description='test_description',
            location='US',
            schema=[schema_field1, schema_field2]
        )
        self.assertEqual(table6_1, table6_2)
        self.assertEqual(table6_1, table6_3)
        self.assertEqual(table6_2, table6_3)

        table7_1 = BigQueryTable(
            table_id='test',
            friendly_name='test_friendly_name',
            description='test_description',
            location='US'
        )
        table7_2 = BigQueryTable(
            table_id='test',
            friendly_name='test_friendly_name',
            description='test_description',
            location='US',
            schema=(schema_field1, )
        )
        table7_3 = BigQueryTable(
            table_id='test',
            friendly_name='test_friendly_name',
            description='test_description',
            location='US',
            schema=(schema_field1, schema_field2)
        )
        self.assertNotEqual(table7_1, table7_2)
        self.assertNotEqual(table7_1, table7_3)
        self.assertNotEqual(table7_2, table7_3)

        label1 = {
            'foo': 'bar'
        }
        label2 = {
            'fizz': 'buzz'
        }

        table8_1 = BigQueryTable(
            table_id='test',
            friendly_name='test_friendly_name',
            description='test_description',
            location='US',
            labels=label1
        )
        table8_2 = BigQueryTable(
            table_id='test',
            friendly_name='test_friendly_name',
            description='test_description',
            location='US',
            labels=label1
        )
        self.assertEqual(table8_1, table8_2)

        table9_1 = BigQueryTable(
            table_id='test',
            friendly_name='test_friendly_name',
            description='test_description',
            location='US',
            labels=label1
        )
        table9_2 = BigQueryTable(
            table_id='test',
            friendly_name='test_friendly_name',
            description='test_description',
            location='US',
            labels=label2
        )
        self.assertNotEqual(table9_1, table9_2)

    def test_from_dict(self):
        expected_table1 = BigQueryTable(
            table_id='test',
            friendly_name='test_friendly_name',
            description='test_description',
            location='US'
        )
        actual_table1_1 = BigQueryTable.from_dict({
            'table_id': 'test',
            'friendly_name': 'test_friendly_name',
            'description': 'test_description',
            'location': 'US'
        })
        self.assertEqual(expected_table1, actual_table1_1)
        actual_table1_2 = BigQueryTable.from_dict({
            'table_id': 'test',
            'friendly_name': 'fizz_buzz',
            'description': 'foo_bar',
            'expires': '2018-01-01T00:00:00.000000+00:00',
            'location': 'EU',
            'partitioning_type': 'DAY'
        })
        self.assertNotEqual(expected_table1, actual_table1_2)

        expected_table2 = BigQueryTable(
            table_id='test',
            friendly_name='test_friendly_name',
            description='test_description',
            view_use_legacy_sql=False,
            view_query='SELECT * FROM bigquery_datasetmanager.test.test'
        )
        actual_table2_1 = BigQueryTable.from_dict({
            'table_id': 'test',
            'friendly_name': 'test_friendly_name',
            'description': 'test_description',
            'view_use_legacy_sql': False,
            'view_query': 'SELECT * FROM bigquery_datasetmanager.test.test'
        })
        self.assertEqual(expected_table2, actual_table2_1)
        actual_table2_2 = BigQueryTable.from_dict({
            'table_id': 'test',
            'friendly_name': 'test_friendly_name',
            'description': 'test_description',
            'view_use_legacy_sql': True,
            'view_query': 'SELECT * FROM bigquery_datasetmanager.foo.bar'
        })
        self.assertNotEqual(expected_table2, actual_table2_2)

        expected_table3 = BigQueryTable(
            table_id='test',
            friendly_name='test_friendly_name',
            description='test_description',
            location='US',
            schema=(
                BigQuerySchemaField(
                    'test',
                    'INTEGER',
                    'NULLABLE',
                    'test_description',
                    None
                ),
            )
        )
        actual_table3_1 = BigQueryTable.from_dict({
            'table_id': 'test',
            'friendly_name': 'test_friendly_name',
            'description': 'test_description',
            'location': 'US',
            'schema': [
                {
                    'name': 'test',
                    'field_type': 'INTEGER',
                    'mode': 'NULLABLE',
                    'description': 'test_description',
                    'fields': None
                }
            ]
        })
        self.assertEqual(expected_table3, actual_table3_1)
        actual_table3_2 = BigQueryTable.from_dict({
            'table_id': 'test',
            'friendly_name': 'test_friendly_name',
            'description': 'test_description',
            'location': 'US',
            'schema': [
                {
                    'name': 'test',
                    'field_type': 'STRING',
                    'mode': 'REQUIRED',
                    'description': 'foo_bar',
                    'fields': None
                }
            ]
        })
        self.assertNotEqual(expected_table3, actual_table3_2)

        expected_table4 = BigQueryTable(
            table_id='test',
            friendly_name='test_friendly_name',
            description='test_description',
            location='US',
            labels={
                'foo': 'bar'
            }
        )
        actual_table4_1 = BigQueryTable.from_dict({
            'table_id': 'test',
            'friendly_name': 'test_friendly_name',
            'description': 'test_description',
            'location': 'US',
            'labels': {
                'foo': 'bar'
            }
        })
        self.assertEqual(expected_table4, actual_table4_1)
        actual_table4_2 = BigQueryTable.from_dict({
            'table_id': 'test',
            'friendly_name': 'test_friendly_name',
            'description': 'test_description',
            'location': 'US',
            'labels': {
                'foo': 'bar',
                'fizz': 'buzz'
            }
        })
        self.assertNotEqual(expected_table4, actual_table4_2)

    def test_from_table(self):
        project = 'test'

        expected_table1 = BigQueryTable(
            table_id='test',
            friendly_name='test_friendly_name',
            description='test_description',
            location='US'
        )
        actual_table1_1 = BigQueryTable.from_table(make_table(
            project=project,
            dataset_id='test',
            table_id='test',
            friendly_name='test_friendly_name',
            description='test_description',
            location='US',
        ))
        self.assertEqual(expected_table1, actual_table1_1)
        actual_table1_2 = BigQueryTable.from_table(make_table(
            project=project,
            dataset_id='test',
            table_id='test',
            friendly_name='foo_bar',
            description='fizz_buzz',
            location='EU'
        ))
        self.assertNotEqual(expected_table1, actual_table1_2)

        expected_table2 = BigQueryTable(
            table_id='test',
            friendly_name='test_friendly_name',
            description='test_description',
            expires=datetime(2018, 1, 1, 0, 0, 0, tzinfo=UTC),
            location='US',
            partitioning_type='DAY'
        )
        actual_table2_1 = BigQueryTable.from_table(make_table(
            project=project,
            dataset_id='test',
            table_id='test',
            friendly_name='test_friendly_name',
            description='test_description',
            expires=datetime(2018, 1, 1, 0, 0, 0, tzinfo=UTC),
            location='US',
            partitioning_type='DAY'
        ))
        self.assertEqual(expected_table2, actual_table2_1)
        actual_table2_2 = BigQueryTable.from_table(make_table(
            project=project,
            dataset_id='test',
            table_id='test',
            friendly_name='test_friendly_name',
            description='test_description',
            expires=datetime(2019, 1, 1, 0, 0, 0, tzinfo=UTC),
            location='US',
            partitioning_type=None
        ))
        self.assertNotEqual(expected_table2, actual_table2_2)

        expected_table3 = BigQueryTable(
            table_id='test',
            friendly_name='test_friendly_name',
            description='test_description',
            view_use_legacy_sql=False,
            view_query='SELECT * FROM bigquery_datasetmanager.test.test'
        )
        actual_table3_1 = BigQueryTable.from_table(make_table(
            project=project,
            dataset_id='test',
            table_id='test',
            friendly_name='test_friendly_name',
            description='test_description',
            view_use_legacy_sql=False,
            view_query='SELECT * FROM bigquery_datasetmanager.test.test'
        ))
        self.assertEqual(expected_table3, actual_table3_1)
        actual_table3_2 = BigQueryTable.from_table(make_table(
            project=project,
            dataset_id='test',
            table_id='test',
            friendly_name='test_friendly_name',
            description='test_description',
            view_use_legacy_sql=True,
            view_query='SELECT * FROM bigquery_datasetmanager.foo.bar'
        ))
        self.assertNotEqual(expected_table3, actual_table3_2)

        expected_table4 = BigQueryTable(
            table_id='test',
            friendly_name='test_friendly_name',
            description='test_description',
            location='US',
            schema=(
                BigQuerySchemaField(
                    name='test',
                    field_type='INTEGER',
                    mode='NULLABLE',
                    description='test_description'
                ),
            ),
            labels=None
        )
        actual_table4_1 = BigQueryTable.from_table(make_table(
            project=project,
            dataset_id='test',
            table_id='test',
            friendly_name='test_friendly_name',
            description='test_description',
            location='US',
            schema=(
                SchemaField(
                    name='test',
                    field_type='INTEGER',
                    mode='NULLABLE',
                    description='test_description'
                ),
            )
        ))
        self.assertEqual(expected_table4, actual_table4_1)
        actual_table4_2 = BigQueryTable.from_table(make_table(
            project=project,
            dataset_id='test',
            table_id='test',
            friendly_name='test_friendly_name',
            description='test_description',
            location='US',
            schema=(
                SchemaField(
                    name='test',
                    field_type='STRING',
                    mode='REQUIRED',
                    description='foo_bar'
                ),
            )
        ))
        self.assertNotEqual(expected_table4, actual_table4_2)

        expected_table5 = BigQueryTable(
            table_id='test',
            friendly_name='test_friendly_name',
            description='test_description',
            location='US',
            labels={
                'foo': 'bar'
            }
        )
        actual_table5_1 = BigQueryTable.from_table(make_table(
            project=project,
            dataset_id='test',
            table_id='test',
            friendly_name='test_friendly_name',
            description='test_description',
            location='US',
            labels={
                'foo': 'bar'
            }
        ))
        self.assertEqual(expected_table5, actual_table5_1)
        actual_table5_2 = BigQueryTable.from_table(make_table(
            project=project,
            dataset_id='test',
            table_id='test',
            friendly_name='test_friendly_name',
            description='test_description',
            location='US',
            labels={
                'foo': 'bar',
                'fizz': 'buzz'
            }
        ))
        self.assertNotEqual(expected_table5, actual_table5_2)

    def test_to_table(self):
        project = 'test'
        dataset_ref = DatasetReference(project, 'test')

        expected_table1 = make_table(
            project=project,
            dataset_id='test',
            table_id='test',
            friendly_name='test_friendly_name',
            description='test_description',
            location='US',
        )
        actual_table1_1 = BigQueryTable.to_table(dataset_ref, BigQueryTable(
            table_id='test',
            friendly_name='test_friendly_name',
            description='test_description',
            location='US'
        ))
        self.assertEqual(expected_table1.table_id, actual_table1_1.table_id)
        self.assertEqual(expected_table1.friendly_name, actual_table1_1.friendly_name)
        self.assertEqual(expected_table1.description, actual_table1_1.description)
        self.assertEqual(expected_table1.location, actual_table1_1.location)
        actual_table1_2 = BigQueryTable.to_table(dataset_ref, BigQueryTable(
            table_id='aaa',
            friendly_name='foo_bar',
            description='fizz_buzz',
            location='EU'
        ))
        self.assertNotEqual(expected_table1.table_id, actual_table1_2.table_id)
        self.assertNotEqual(expected_table1.friendly_name, actual_table1_2.friendly_name)
        self.assertNotEqual(expected_table1.description, actual_table1_2.description)
        self.assertNotEqual(expected_table1.location, actual_table1_2.location)

        expected_table2 = make_table(
            project=project,
            dataset_id='test',
            table_id='test',
            friendly_name='test_friendly_name',
            description='test_description',
            expires=datetime(2018, 1, 1, 0, 0, 0, tzinfo=UTC),
            location='US',
            partitioning_type='DAY'
        )
        actual_table2_1 = BigQueryTable.to_table(dataset_ref, BigQueryTable(
            table_id='test',
            friendly_name='test_friendly_name',
            description='test_description',
            expires=datetime(2018, 1, 1, 0, 0, 0, tzinfo=UTC),
            location='US',
            partitioning_type='DAY'
        ))
        self.assertEqual(expected_table2.expires, actual_table2_1.expires)
        self.assertEqual(expected_table2.partitioning_type, actual_table2_1.partitioning_type)
        actual_table2_2 = BigQueryTable.to_table(dataset_ref, BigQueryTable(
            table_id='test',
            friendly_name='test_friendly_name',
            description='description',
            expires=datetime(2019, 1, 1, 0, 0, 0, tzinfo=UTC),
            location='US',
            partitioning_type=None
        ))
        self.assertNotEqual(expected_table2.expires, actual_table2_2.expires)
        self.assertNotEqual(expected_table2.partitioning_type, actual_table2_2.partitioning_type)

        expected_table3 = make_table(
            project=project,
            dataset_id='test',
            table_id='test',
            friendly_name='test_friendly_name',
            description='test_description',
            view_use_legacy_sql=False,
            view_query='SELECT * FROM bigquery_datasetmanager.test.test'
        )
        actual_table3_1 = BigQueryTable.to_table(dataset_ref, BigQueryTable(
            table_id='test',
            friendly_name='test_friendly_name',
            description='test_description',
            view_use_legacy_sql=False,
            view_query='SELECT * FROM bigquery_datasetmanager.test.test'
        ))
        self.assertEqual(expected_table3.view_use_legacy_sql, actual_table3_1.view_use_legacy_sql)
        self.assertEqual(expected_table3.view_query, actual_table3_1.view_query)
        actual_table3_2 = BigQueryTable.to_table(dataset_ref, BigQueryTable(
            table_id='test',
            friendly_name='test_friendly_name',
            description='test_description',
            view_use_legacy_sql=True,
            view_query='SELECT * FROM bigquery_datasetmanager.foo.bar'
        ))
        self.assertNotEqual(expected_table3.view_use_legacy_sql, actual_table3_2.view_use_legacy_sql)
        self.assertNotEqual(expected_table3.view_query, actual_table3_2.view_query)

        expected_table4 = make_table(
            project=project,
            dataset_id='test',
            table_id='test',
            friendly_name='test_friendly_name',
            description='test_description',
            location='US',
            schema=(
                SchemaField(
                    name='test',
                    field_type='INTEGER',
                    mode='NULLABLE',
                    description='test_description'
                ),
            )
        )
        actual_table4_1 = BigQueryTable.to_table(dataset_ref, BigQueryTable(
            table_id='test',
            friendly_name='test_friendly_name',
            description='test_description',
            location='US',
            schema=(
                BigQuerySchemaField(
                    'test',
                    'INTEGER',
                    'NULLABLE',
                    'test_description',
                    None
                ),
            )
        ))
        self.assertEqual(expected_table4.schema, actual_table4_1.schema)
        actual_table4_2 = BigQueryTable.to_table(dataset_ref, BigQueryTable(
            table_id='test',
            friendly_name='test_friendly_name',
            description='test_description',
            location='US',
            schema=(
                SchemaField(
                    name='test',
                    field_type='STRING',
                    mode='REQUIRED',
                    description='foo_bar'
                ),
            )
        ))
        self.assertNotEqual(expected_table4.schema, actual_table4_2.schema)

        expected_table5 = make_table(
            project=project,
            dataset_id='test',
            table_id='test',
            friendly_name='test_friendly_name',
            description='test_description',
            location='US',
            labels={
                'foo': 'bar'
            }
        )
        actual_table5_1 = BigQueryTable.to_table(dataset_ref, BigQueryTable(
            table_id='test',
            friendly_name='test_friendly_name',
            description='test_description',
            location='US',
            labels={
                'foo': 'bar'
            }
        ))
        self.assertEqual(expected_table5.labels, actual_table5_1.labels)
        actual_table5_2 = BigQueryTable.to_table(dataset_ref, BigQueryTable(
            table_id='test',
            friendly_name='test_friendly_name',
            description='test_description',
            location='US',
            labels={
                'foo': 'bar',
                'fizz': 'buzz'
            }
        ))
        self.assertNotEqual(expected_table5.labels, actual_table5_2.labels)
