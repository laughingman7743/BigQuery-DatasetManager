# -*- coding: utf-8 -*-
from __future__ import absolute_import

import unittest
from datetime import datetime

from pytz import UTC

from bqdm.model.dataset import BigQueryAccessEntry, BigQueryDataset
from bqdm.model.schema import BigQuerySchemaField
from bqdm.model.table import BigQueryTable
from bqdm.util import dump


class TestUtil(unittest.TestCase):

    def test_dump_dataset(self):
        dataset1 = BigQueryDataset(
            dataset_id='test1',
            friendly_name='test_friendly_name',
            description='test_description',
            default_table_expiration_ms=24 * 30 * 60 * 1000,
            location='US'
        )
        expected_dump_data1 = """dataset_id: test1
friendly_name: test_friendly_name
description: test_description
default_table_expiration_ms: 43200000
location: US
access_entries: null
labels: null
"""
        actual_dump_data1 = dump(dataset1)
        self.assertEqual(expected_dump_data1, actual_dump_data1)

        access_entry2 = BigQueryAccessEntry(
            'OWNER',
            'specialGroup',
            'projectOwners'
        )
        dataset2 = BigQueryDataset(
            dataset_id='test2',
            friendly_name='test_friendly_name',
            description='test_description',
            default_table_expiration_ms=24 * 30 * 60 * 1000,
            location='US',
            access_entries=(access_entry2, )
        )
        expected_dump_data2 = """dataset_id: test2
friendly_name: test_friendly_name
description: test_description
default_table_expiration_ms: 43200000
location: US
access_entries:
-   role: OWNER
    entity_type: specialGroup
    entity_id: projectOwners
labels: null
"""
        actual_dump_data2 = dump(dataset2)
        self.assertEqual(expected_dump_data2, actual_dump_data2)

        access_entry3 = BigQueryAccessEntry(
            None,
            'view',
            {
                'datasetId': 'test',
                'projectId': 'test-project',
                'tableId': 'test_table'
            }
        )
        dataset3 = BigQueryDataset(
            dataset_id='test3',
            friendly_name='test_friendly_name',
            description='test_description',
            default_table_expiration_ms=24 * 30 * 60 * 1000,
            location='US',
            access_entries=(access_entry3, )
        )
        expected_dump_data3 = """dataset_id: test3
friendly_name: test_friendly_name
description: test_description
default_table_expiration_ms: 43200000
location: US
access_entries:
-   role: null
    entity_type: view
    entity_id:
        datasetId: test
        projectId: test-project
        tableId: test_table
labels: null
"""
        actual_dump_data3 = dump(dataset3)
        self.assertEqual(expected_dump_data3, actual_dump_data3)

        dataset4 = BigQueryDataset(
            dataset_id='test4',
            friendly_name='test_friendly_name',
            description='test_description',
            default_table_expiration_ms=24 * 30 * 60 * 1000,
            location='US',
            access_entries=(access_entry2, access_entry3)
        )
        expected_dump_data4 = """dataset_id: test4
friendly_name: test_friendly_name
description: test_description
default_table_expiration_ms: 43200000
location: US
access_entries:
-   role: OWNER
    entity_type: specialGroup
    entity_id: projectOwners
-   role: null
    entity_type: view
    entity_id:
        datasetId: test
        projectId: test-project
        tableId: test_table
labels: null
"""
        actual_dump_data4 = dump(dataset4)
        self.assertEqual(expected_dump_data4, actual_dump_data4)

        label5 = {
            'foo': 'bar'
        }
        dataset5 = BigQueryDataset(
            dataset_id='test5',
            friendly_name='test_friendly_name',
            description='test_description',
            default_table_expiration_ms=24 * 30 * 60 * 1000,
            location='US',
            labels=label5
        )
        expected_dump_data5 = """dataset_id: test5
friendly_name: test_friendly_name
description: test_description
default_table_expiration_ms: 43200000
location: US
access_entries: null
labels:
    foo: bar
"""
        actual_dump_data5 = dump(dataset5)
        self.assertEqual(expected_dump_data5, actual_dump_data5)

        label6 = {
            'aaa': 'bbb',
            'ccc': 'ddd'
        }
        dataset6 = BigQueryDataset(
            dataset_id='test6',
            friendly_name='test_friendly_name',
            description='test_description',
            default_table_expiration_ms=24 * 30 * 60 * 1000,
            location='US',
            labels=label6
        )
        expected_dump_data6 = """dataset_id: test6
friendly_name: test_friendly_name
description: test_description
default_table_expiration_ms: 43200000
location: US
access_entries: null
labels:
    aaa: bbb
    ccc: ddd
"""
        actual_dump_data6 = dump(dataset6)
        self.assertEqual(expected_dump_data6, actual_dump_data6)

    def test_dump_table(self):
        table1 = BigQueryTable(
            table_id='test',
            friendly_name='test_friendly_name',
            description='test_description',
            location='US'
        )
        expected_dump_data1 = """table_id: test
friendly_name: test_friendly_name
description: test_description
expires: null
location: US
partitioning_type: null
view_use_legacy_sql: null
view_query: null
schema: null
labels: null
"""
        actual_dump_data1 = dump(table1)
        self.assertEqual(expected_dump_data1, actual_dump_data1)

        table2 = BigQueryTable(
            table_id='test',
            friendly_name='fizz_buzz',
            description='foo_bar',
            expires=datetime(2018, 1, 1, 0, 0, 0, tzinfo=UTC),
            location='EU',
            partitioning_type='DAY',
        )
        expected_dump_data2 = """table_id: test
friendly_name: fizz_buzz
description: foo_bar
expires: 2018-01-01T00:00:00.000000+0000
location: EU
partitioning_type: DAY
view_use_legacy_sql: null
view_query: null
schema: null
labels: null
"""
        actual_dump_data2 = dump(table2)
        self.assertEqual(expected_dump_data2, actual_dump_data2)

        table3 = BigQueryTable(
            table_id='test',
            friendly_name='test_friendly_name',
            description='test_description',
            view_use_legacy_sql=False,
            view_query="""SELECT * FROM
bigquery_datasetmanager.test.test"""
        )
        expected_dump_data3 = """table_id: test
friendly_name: test_friendly_name
description: test_description
expires: null
location: null
partitioning_type: null
view_use_legacy_sql: false
view_query: |-
    SELECT * FROM
    bigquery_datasetmanager.test.test
schema: null
labels: null
"""
        actual_dump_data3 = dump(table3)
        self.assertEqual(expected_dump_data3, actual_dump_data3)

        schema_field1 = BigQuerySchemaField(
            name='test1',
            field_type='INTEGER',
            mode='NULLABLE',
            description='test_description'
        )
        schema_field2 = BigQuerySchemaField(
            name='test2',
            field_type='STRING',
            mode='REQUIRED',
            description='foo_bar'
        )
        schema_field3 = BigQuerySchemaField(
            name='test3',
            field_type='RECORD',
            mode='NULLABLE',
            description='fizz_buzz',
            fields=(schema_field1, schema_field2)
        )
        schema_field4 = BigQuerySchemaField(
            name='test4',
            field_type='RECORD',
            mode='NULLABLE',
            description='aaa_bbb',
            fields=(schema_field3, )
        )

        table4 = BigQueryTable(
            table_id='test',
            friendly_name='test_friendly_name',
            description='test_description',
            location='US',
            schema=(schema_field1, )
        )
        expected_dump_data4 = """table_id: test
friendly_name: test_friendly_name
description: test_description
expires: null
location: US
partitioning_type: null
view_use_legacy_sql: null
view_query: null
schema:
-   name: test1
    field_type: INTEGER
    mode: NULLABLE
    description: test_description
    fields: null
labels: null
"""
        actual_dump_data4 = dump(table4)
        self.assertEqual(expected_dump_data4, actual_dump_data4)

        table5 = BigQueryTable(
            table_id='test',
            friendly_name='test_friendly_name',
            description='test_description',
            location='US',
            schema=(schema_field1, schema_field2)
        )
        expected_dump_data5 = """table_id: test
friendly_name: test_friendly_name
description: test_description
expires: null
location: US
partitioning_type: null
view_use_legacy_sql: null
view_query: null
schema:
-   name: test1
    field_type: INTEGER
    mode: NULLABLE
    description: test_description
    fields: null
-   name: test2
    field_type: STRING
    mode: REQUIRED
    description: foo_bar
    fields: null
labels: null
"""
        actual_dump_data5 = dump(table5)
        self.assertEqual(expected_dump_data5, actual_dump_data5)

        table6 = BigQueryTable(
            table_id='test',
            friendly_name='test_friendly_name',
            description='test_description',
            location='US',
            schema=(schema_field3, )
        )
        expected_dump_data6 = """table_id: test
friendly_name: test_friendly_name
description: test_description
expires: null
location: US
partitioning_type: null
view_use_legacy_sql: null
view_query: null
schema:
-   name: test3
    field_type: RECORD
    mode: NULLABLE
    description: fizz_buzz
    fields:
    -   name: test1
        field_type: INTEGER
        mode: NULLABLE
        description: test_description
        fields: null
    -   name: test2
        field_type: STRING
        mode: REQUIRED
        description: foo_bar
        fields: null
labels: null
"""
        actual_dump_data6 = dump(table6)
        self.assertEqual(expected_dump_data6, actual_dump_data6)

        table7 = BigQueryTable(
            table_id='test',
            friendly_name='test_friendly_name',
            description='test_description',
            location='US',
            schema=(schema_field4, )
        )
        expected_dump_data7 = """table_id: test
friendly_name: test_friendly_name
description: test_description
expires: null
location: US
partitioning_type: null
view_use_legacy_sql: null
view_query: null
schema:
-   name: test4
    field_type: RECORD
    mode: NULLABLE
    description: aaa_bbb
    fields:
    -   name: test3
        field_type: RECORD
        mode: NULLABLE
        description: fizz_buzz
        fields:
        -   name: test1
            field_type: INTEGER
            mode: NULLABLE
            description: test_description
            fields: null
        -   name: test2
            field_type: STRING
            mode: REQUIRED
            description: foo_bar
            fields: null
labels: null
"""
        actual_dump_data7 = dump(table7)
        self.assertEqual(expected_dump_data7, actual_dump_data7)

        label1 = {
            'foo': 'bar'
        }
        label2 = {
            'fizz': 'buzz',
            'aaa': 'bbb'
        }

        table8 = BigQueryTable(
            table_id='test',
            friendly_name='test_friendly_name',
            description='test_description',
            location='US',
            labels=label1
        )
        expected_dump_data8 = """table_id: test
friendly_name: test_friendly_name
description: test_description
expires: null
location: US
partitioning_type: null
view_use_legacy_sql: null
view_query: null
schema: null
labels:
    foo: bar
"""
        actual_dump_data8 = dump(table8)
        self.assertEqual(expected_dump_data8, actual_dump_data8)

        table9 = BigQueryTable(
            table_id='test',
            friendly_name='test_friendly_name',
            description='test_description',
            location='US',
            labels=label2
        )
        expected_dump_data9 = """table_id: test
friendly_name: test_friendly_name
description: test_description
expires: null
location: US
partitioning_type: null
view_use_legacy_sql: null
view_query: null
schema: null
labels:
    aaa: bbb
    fizz: buzz
"""
        actual_dump_data9 = dump(table9)
        self.assertEqual(expected_dump_data9, actual_dump_data9)
