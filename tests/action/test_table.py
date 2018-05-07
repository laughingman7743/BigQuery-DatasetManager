# -*- coding: utf-8 -*-
from __future__ import absolute_import

import unittest
from datetime import datetime

from pytz import UTC

from bqdm.action.table import TableAction
from bqdm.model.schema import BigQuerySchemaField
from bqdm.model.table import BigQueryTable


class TestTableAction(unittest.TestCase):

    def test_get_add_tables(self):
        source_table1_1 = BigQueryTable(
            table_id='test1',
            friendly_name='test_friendly_name',
            description='test_description',
            location='US'
        )
        source_table1_2 = BigQueryTable(
            table_id='test2',
            friendly_name='foo_bar',
            description='fizz_buzz',
            location='EU'
        )

        target_table1_1 = BigQueryTable(
            table_id='test1',
            friendly_name='test_friendly_name',
            description='test_description',
            location='US'
        )
        target_table1_2 = BigQueryTable(
            table_id='test2',
            friendly_name='foo_bar',
            description='fizz_buzz',
            location='EU'
        )

        source1 = [source_table1_1]
        target1 = [target_table1_1, target_table1_2]
        actual_count1, actual_results1 = TableAction.get_add_tables(source1, target1)
        self.assertEqual(actual_count1, 1)
        self.assertEqual(actual_results1, (target_table1_2, ))

        source2 = [source_table1_1, source_table1_2]
        target2 = [target_table1_1, target_table1_2]
        actual_count2, actual_results2 = TableAction.get_add_tables(source2, target2)
        self.assertEqual(actual_count2, 0)
        self.assertEqual(actual_results2, ())

        source3 = [source_table1_1, source_table1_2]
        target3 = [target_table1_1]
        actual_count3, actual_results3 = TableAction.get_add_tables(source3, target3)
        self.assertEqual(actual_count3, 0)
        self.assertEqual(actual_results3, ())

    def test_get_change_tables(self):
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

        label1 = {
            'foo': 'bar'
        }
        label2 = {
            'fizz': 'buzz'
        }

        source_table1_1 = BigQueryTable(
            table_id='test1',
            friendly_name='test_friendly_name',
            description='test_description',
            location='US'
        )
        source_table1_2 = BigQueryTable(
            table_id='test2',
            friendly_name='test_friendly_name',
            description='test_description',
            expires=datetime(2018, 1, 1, 0, 0, 0, tzinfo=UTC),
            location='US',
            partitioning_type='DAY'
        )
        source_table1_3 = BigQueryTable(
            table_id='test3',
            friendly_name='test_friendly_name',
            description='test_description',
            view_use_legacy_sql=False,
            view_query='SELECT * FROM bigquery_datasetmanager.test.test'
        )
        source_table1_4 = BigQueryTable(
            table_id='test4',
            friendly_name='test_friendly_name',
            description='test_description',
            location='US',
            schema=(schema_field1, )
        )
        source_table1_5 = BigQueryTable(
            table_id='test5',
            friendly_name='test_friendly_name',
            description='test_description',
            location='US',
            labels=label1
        )

        target_table1_1 = BigQueryTable(
            table_id='test1',
            friendly_name='foo_bar',
            description='fizz_buzz',
            location='EU'
        )
        target_table1_2 = BigQueryTable(
            table_id='test2',
            friendly_name='test_friendly_name',
            description='test_description',
            expires=datetime(2019, 1, 1, 0, 0, 0, tzinfo=UTC),
            location='US'
        )
        target_table1_3 = BigQueryTable(
            table_id='test3',
            friendly_name='test_friendly_name',
            description='test_description',
            view_use_legacy_sql=True,
            view_query='SELECT * FROM bigquery_datasetmanager.foo.bar'
        )
        target_table1_4 = BigQueryTable(
            table_id='test4',
            friendly_name='test_friendly_name',
            description='test_description',
            location='US',
            schema=(schema_field2, )
        )
        target_table1_5 = BigQueryTable(
            table_id='test5',
            friendly_name='test_friendly_name',
            description='test_description',
            location='US',
            labels=label2
        )

        source1 = [source_table1_1, source_table1_2, source_table1_3,
                   source_table1_4, source_table1_5]
        target1 = [target_table1_1, target_table1_2, target_table1_3,
                   target_table1_4, target_table1_5]
        actual_count1, actual_results1 = TableAction.get_change_tables(source1, target1)
        self.assertEqual(actual_count1, 5)
        self.assertEqual(set(actual_results1),
                         {target_table1_1, target_table1_2, target_table1_3,
                          target_table1_4, target_table1_5})

        source2 = [source_table1_5, source_table1_4, source_table1_3,
                   source_table1_2, source_table1_1]
        target2 = [target_table1_1, target_table1_2, target_table1_3,
                   target_table1_4, target_table1_5]
        actual_count2, actual_results2 = TableAction.get_change_tables(source2, target2)
        self.assertEqual(actual_count2, 5)
        self.assertEqual(set(actual_results2),
                         {target_table1_1, target_table1_2, target_table1_3,
                          target_table1_4, target_table1_5})

        source3 = [source_table1_1, source_table1_2, source_table1_3,
                   source_table1_4, source_table1_5]
        target3 = [source_table1_1, source_table1_2, source_table1_3,
                   source_table1_4, source_table1_5]
        actual_count3, actual_results3 = TableAction.get_change_tables(source3, target3)
        self.assertEqual(actual_count3, 0)
        self.assertEqual(actual_results3, ())

        source4 = [source_table1_1, source_table1_2, source_table1_3,
                   source_table1_4, source_table1_5]
        target4 = [source_table1_5, source_table1_4, source_table1_3,
                   source_table1_2, source_table1_1]
        actual_count4, actual_results4 = TableAction.get_change_tables(source4, target4)
        self.assertEqual(actual_count4, 0)
        self.assertEqual(actual_results4, ())

        source5 = [target_table1_1, target_table1_2, target_table1_3,
                   target_table1_4, target_table1_5]
        target5 = [target_table1_1, target_table1_2, target_table1_3,
                   target_table1_4, target_table1_5]
        actual_count5, actual_results5 = TableAction.get_change_tables(source5, target5)
        self.assertEqual(actual_count5, 0)
        self.assertEqual(actual_results5, ())

        source6 = [target_table1_1, target_table1_2, target_table1_3,
                   target_table1_4, target_table1_5]
        target6 = [target_table1_5, target_table1_4, target_table1_3,
                   target_table1_2, target_table1_1]
        actual_count6, actual_results6 = TableAction.get_change_tables(source6, target6)
        self.assertEqual(actual_count6, 0)
        self.assertEqual(actual_results6, ())

        source7 = [source_table1_1, source_table1_2, source_table1_3,
                   source_table1_4, source_table1_5]
        target7 = [target_table1_1, target_table1_2]
        actual_count7, actual_results7 = TableAction.get_change_tables(source7, target7)
        self.assertEqual(actual_count7, 2)
        self.assertEqual(set(actual_results7), {target_table1_1, target_table1_2})

        source8 = [source_table1_1, source_table1_2, source_table1_3,
                   source_table1_4, source_table1_5]
        target8 = [target_table1_3, target_table1_4, target_table1_5]
        actual_count8, actual_results8 = TableAction.get_change_tables(source8, target8)
        self.assertEqual(actual_count8, 3)
        self.assertEqual(set(actual_results8),
                         {target_table1_3, target_table1_4, target_table1_5})

        source9 = [source_table1_1, source_table1_2]
        target9 = [target_table1_1, target_table1_2, target_table1_3,
                   target_table1_4, target_table1_5]
        actual_count9, actual_results9 = TableAction.get_change_tables(source9, target9)
        self.assertEqual(actual_count9, 2)
        self.assertEqual(set(actual_results9), {target_table1_1, target_table1_2})

        source10 = [source_table1_3, source_table1_4, source_table1_5]
        target10 = [target_table1_1, target_table1_2, target_table1_3,
                    target_table1_4, target_table1_5]
        actual_count10, actual_results10 = TableAction.get_change_tables(source10, target10)
        self.assertEqual(actual_count10, 3)
        self.assertEqual(set(actual_results10),
                         {target_table1_3, target_table1_4, target_table1_5})

    def test_get_destroy_tables(self):
        source_table1_1 = BigQueryTable(
            table_id='test1',
            friendly_name='test_friendly_name',
            description='test_description',
            location='US'
        )
        source_table1_2 = BigQueryTable(
            table_id='test2',
            friendly_name='foo_bar',
            description='fizz_buzz',
            location='EU'
        )

        target_table1_1 = BigQueryTable(
            table_id='test1',
            friendly_name='test_friendly_name',
            description='test_description',
            location='US'
        )
        target_table1_2 = BigQueryTable(
            table_id='test2',
            friendly_name='foo_bar',
            description='fizz_buzz',
            location='EU'
        )

        source1 = [source_table1_1]
        target1 = [target_table1_1, target_table1_2]
        actual_count1, actual_results1 = TableAction.get_destroy_tables(source1, target1)
        self.assertEqual(actual_count1, 0)
        self.assertEqual(actual_results1, ())

        source2 = [source_table1_1, source_table1_2]
        target2 = [target_table1_1, target_table1_2]
        actual_count2, actual_results2 = TableAction.get_destroy_tables(source2, target2)
        self.assertEqual(actual_count2, 0)
        self.assertEqual(actual_results2, ())

        source3 = [source_table1_1, source_table1_2]
        target3 = [target_table1_1]
        actual_count3, actual_results3 = TableAction.get_destroy_tables(source3, target3)
        self.assertEqual(actual_count3, 1)
        self.assertEqual(actual_results3, (source_table1_2, ))

        source4 = [source_table1_1, source_table1_2]
        target4 = []
        actual_count4, actual_results4 = TableAction.get_destroy_tables(source4, target4)
        self.assertEqual(actual_count4, 2)
        self.assertEqual(set(actual_results4), {source_table1_1, source_table1_2, })

        source5 = []
        target5 = [target_table1_1, target_table1_2]
        actual_count5, actual_results5 = TableAction.get_destroy_tables(source5, target5)
        self.assertEqual(actual_count5, 0)
        self.assertEqual(actual_results5, ())

    def test_build_query_field(self):
        source_schema_field1 = BigQuerySchemaField(
            name='test1',
            field_type='STRING',
            mode='NULLABLE',
            description='test_description'
        )
        source_schema_field2 = BigQuerySchemaField(
            name='test2',
            field_type='INTEGER',
            mode='NULLABLE',
            description='test_description'
        )
        source_schema_field3 = BigQuerySchemaField(
            name='test3',
            field_type='RECORD',
            mode='NULLABLE',
            description='test_description',
            fields=(
                BigQuerySchemaField(
                    name='foo_bar',
                    field_type='STRING',
                    mode='NULLABLE',
                    description='test_description'
                ),
            )
        )
        source_schema_field4 = BigQuerySchemaField(
            name='test4',
            field_type='RECORD',
            mode='NULLABLE',
            description='test_description',
            fields=(
                BigQuerySchemaField(
                    name='fizz',
                    field_type='INTEGER',
                    mode='NULLABLE',
                    description='test_description'
                ),
                BigQuerySchemaField(
                    name='buzz',
                    field_type='BOOL',
                    mode='NULLABLE',
                    description='test_description'
                ),
            )
        )

        target_schema_field1 = BigQuerySchemaField(
            name='test1',
            field_type='INTEGER',
            mode='NULLABLE',
            description='test_description'
        )
        target_schema_field2 = BigQuerySchemaField(
            name='test2',
            field_type='STRING',
            mode='NULLABLE',
            description='test_description'
        )
        target_schema_field3 = BigQuerySchemaField(
            name='test3',
            field_type='RECORD',
            mode='NULLABLE',
            description='test_description',
            fields=(
                BigQuerySchemaField(
                    name='foo_bar',
                    field_type='INTEGER',
                    mode='NULLABLE',
                    description='test_description'
                ),
            )
        )
        target_schema_field4 = BigQuerySchemaField(
            name='test4',
            field_type='RECORD',
            mode='NULLABLE',
            description='test_description',
            fields=(
                BigQuerySchemaField(
                    name='fizz',
                    field_type='FLOAT',
                    mode='NULLABLE',
                    description='test_description'
                ),
                BigQuerySchemaField(
                    name='buzz',
                    field_type='STRING',
                    mode='NULLABLE',
                    description='test_description'
                ),
            )
        )

        source1 = (source_schema_field1, source_schema_field2,
                   source_schema_field3, source_schema_field4)
        target1 = (target_schema_field1, target_schema_field2,
                   target_schema_field3, target_schema_field4)

        expected_query_field = 'cast(test1 AS INT64) AS test1, ' \
                               'cast(test2 AS STRING) AS test2, ' \
                               'struct(cast(test3.foo_bar AS INT64) AS foo_bar) AS test3, ' \
                               'struct(cast(test4.fizz AS FLOAT64) AS fizz, ' \
                               'cast(test4.buzz AS STRING) AS buzz) AS test4'
        actual_query_field = TableAction.build_query_field(source1, target1)
        self.assertEqual(expected_query_field, actual_query_field)

    # TODO
    # test_plan_add
    # test_add
    # test_plan_change
    # test_change
    # test_plan_destroy
    # test_destroy

    # TODO
    # test_backup
    # test_select_insert
    # test_create_temporary_table

    # TODO
    # test_list_tables
    # test_export
