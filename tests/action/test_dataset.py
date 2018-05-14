# -*- coding: utf-8 -*-
from __future__ import absolute_import

import unittest

from bqdm.action.dataset import DatasetAction
from bqdm.model.dataset import BigQueryAccessEntry, BigQueryDataset


class TestDatasetAction(unittest.TestCase):

    def test_get_add_datasets(self):
        source_dataset1_1 = BigQueryDataset(
            dataset_id='test1',
            friendly_name='test_friendly_name',
            description='test_description',
            default_table_expiration_ms=24 * 60 * 60 * 1000,
            location='US'
        )
        source_dataset1_2 = BigQueryDataset(
            dataset_id='test2',
            friendly_name='foo_bar',
            description='fizz_buzz',
            default_table_expiration_ms=60 * 60 * 1000,
            location='EU'
        )

        target_dataset1_1 = BigQueryDataset(
            dataset_id='test1',
            friendly_name='test_friendly_name',
            description='test_description',
            default_table_expiration_ms=24 * 60 * 60 * 1000,
            location='US'
        )
        target_dataset1_2 = BigQueryDataset(
            dataset_id='test2',
            friendly_name='foo_bar',
            description='fizz_buzz',
            default_table_expiration_ms=60 * 60 * 1000,
            location='EU'
        )

        source1 = [source_dataset1_1]
        target1 = [target_dataset1_1, target_dataset1_2]
        actual_count1, actual_results1 = DatasetAction.get_add_datasets(source1, target1)
        self.assertEqual(actual_count1, 1)
        self.assertEqual(actual_results1, (target_dataset1_2, ))

        source2 = [source_dataset1_1, source_dataset1_2]
        target2 = [target_dataset1_1, target_dataset1_2]
        actual_count2, actual_results2 = DatasetAction.get_add_datasets(source2, target2)
        self.assertEqual(actual_count2, 0)
        self.assertEqual(actual_results2, ())

        source3 = [source_dataset1_1, source_dataset1_2]
        target3 = [target_dataset1_1]
        actual_count3, actual_results3 = DatasetAction.get_add_datasets(source3, target3)
        self.assertEqual(actual_count3, 0)
        self.assertEqual(actual_results3, ())

    def test_get_change_datasets(self):
        source_dataset1_1 = BigQueryDataset(
            dataset_id='test1',
            friendly_name='test_friendly_name',
            description='test_description',
            default_table_expiration_ms=24 * 60 * 60 * 1000,
            location='US'
        )
        source_dataset1_2 = BigQueryDataset(
            dataset_id='test2',
            friendly_name='test_friendly_name',
            description='test_description',
            default_table_expiration_ms=24 * 60 * 60 * 1000,
            location='US',
            access_entries=(
                BigQueryAccessEntry(
                    'OWNER',
                    'specialGroup',
                    'projectOwners'
                ),
            )
        )
        source_dataset1_3 = BigQueryDataset(
            dataset_id='test3',
            friendly_name='test_friendly_name',
            description='test_description',
            default_table_expiration_ms=24 * 30 * 60 * 1000,
            location='US',
            labels={
                'foo': 'bar'
            }
        )

        target_dataset1_1 = BigQueryDataset(
            dataset_id='test1',
            friendly_name='foo',
            description='bar',
            default_table_expiration_ms=60 * 60 * 1000,
            location='UK'
        )
        target_dataset1_2 = BigQueryDataset(
            dataset_id='test2',
            friendly_name='test_friendly_name',
            description='test_description',
            default_table_expiration_ms=24 * 60 * 60 * 1000,
            location='US',
            access_entries=(
                BigQueryAccessEntry(
                    'OWNER',
                    'specialGroup',
                    'projectOwners'
                ),
                BigQueryAccessEntry(
                    None,
                    'view',
                    {
                        'datasetId': 'test',
                        'projectId': 'test',
                        'tableId': 'test_table'
                    }
                )
            )
        )
        target_dataset1_3 = BigQueryDataset(
            dataset_id='test3',
            friendly_name='test_friendly_name',
            description='test_description',
            default_table_expiration_ms=24 * 60 * 60 * 1000,
            location='US',
            labels={
                'aaa': 'bbb',
                'ccc': 'ddd'
            }
        )

        source1 = [source_dataset1_1, source_dataset1_2, source_dataset1_3]
        target1 = [target_dataset1_1, target_dataset1_2, target_dataset1_3]
        actual_count1, actual_results1 = DatasetAction.get_change_datasets(source1, target1)
        self.assertEqual(actual_count1, 3)
        self.assertEqual(set(actual_results1),
                         {target_dataset1_1, target_dataset1_2, target_dataset1_3})

        source2 = [source_dataset1_3, source_dataset1_2, source_dataset1_1]
        target2 = [target_dataset1_1, target_dataset1_2, target_dataset1_3]
        actual_count2, actual_results2 = DatasetAction.get_change_datasets(source2, target2)
        self.assertEqual(actual_count2, 3)
        self.assertEqual(set(actual_results2),
                         {target_dataset1_1, target_dataset1_2, target_dataset1_3})

        source3 = [source_dataset1_1, source_dataset1_2, source_dataset1_3]
        target3 = [source_dataset1_1, source_dataset1_2, source_dataset1_3]
        actual_count3, actual_results3 = DatasetAction.get_change_datasets(source3, target3)
        self.assertEqual(actual_count3, 0)
        self.assertEqual(actual_results3, ())

        source4 = [source_dataset1_1, source_dataset1_2, source_dataset1_3]
        target4 = [source_dataset1_3, source_dataset1_2, source_dataset1_1]
        actual_count4, actual_results4 = DatasetAction.get_change_datasets(source4, target4)
        self.assertEqual(actual_count4, 0)
        self.assertEqual(actual_results4, ())

        source5 = [target_dataset1_1, target_dataset1_2, target_dataset1_3]
        target5 = [target_dataset1_1, target_dataset1_2, target_dataset1_3]
        actual_count5, actual_results5 = DatasetAction.get_change_datasets(source5, target5)
        self.assertEqual(actual_count5, 0)
        self.assertEqual(actual_results5, ())

        source6 = [target_dataset1_1, target_dataset1_2, target_dataset1_3]
        target6 = [target_dataset1_3, target_dataset1_2, target_dataset1_1]
        actual_count6, actual_results6 = DatasetAction.get_change_datasets(source6, target6)
        self.assertEqual(actual_count6, 0)
        self.assertEqual(actual_results6, ())

        source7 = [source_dataset1_1, source_dataset1_2, source_dataset1_3]
        target7 = [target_dataset1_1, target_dataset1_2]
        actual_count7, actual_results7 = DatasetAction.get_change_datasets(source7, target7)
        self.assertEqual(actual_count7, 2)
        self.assertEqual(set(actual_results7), {target_dataset1_1, target_dataset1_2})

        source8 = [source_dataset1_1, source_dataset1_2]
        target8 = [target_dataset1_1, target_dataset1_2, target_dataset1_3]
        actual_count8, actual_results8 = DatasetAction.get_change_datasets(source8, target8)
        self.assertEqual(actual_count8, 2)
        self.assertEqual(set(actual_results8), {target_dataset1_1, target_dataset1_2})

    def test_get_destroy_datasets(self):
        source_dataset1_1 = BigQueryDataset(
            dataset_id='test1',
            friendly_name='test_friendly_name',
            description='test_description',
            default_table_expiration_ms=24 * 60 * 60 * 1000,
            location='US'
        )
        source_dataset1_2 = BigQueryDataset(
            dataset_id='test2',
            friendly_name='foo_bar',
            description='fizz_buzz',
            default_table_expiration_ms=60 * 60 * 1000,
            location='EU'
        )

        target_dataset1_1 = BigQueryDataset(
            dataset_id='test1',
            friendly_name='test_friendly_name',
            description='test_description',
            default_table_expiration_ms=24 * 60 * 60 * 1000,
            location='US'
        )
        target_dataset1_2 = BigQueryDataset(
            dataset_id='test2',
            friendly_name='foo_bar',
            description='fizz_buzz',
            default_table_expiration_ms=60 * 60 * 1000,
            location='EU'
        )

        source1 = [source_dataset1_1]
        target1 = [target_dataset1_1, target_dataset1_2]
        actual_count1, actual_results1 = DatasetAction.get_destroy_datasets(source1, target1)
        self.assertEqual(actual_count1, 0)
        self.assertEqual(actual_results1, ())

        source2 = [source_dataset1_1, source_dataset1_2]
        target2 = [target_dataset1_1, target_dataset1_2]
        actual_count2, actual_results2 = DatasetAction.get_destroy_datasets(source2, target2)
        self.assertEqual(actual_count2, 0)
        self.assertEqual(actual_results2, ())

        source3 = [source_dataset1_1, source_dataset1_2]
        target3 = [target_dataset1_1]
        actual_count3, actual_results3 = DatasetAction.get_destroy_datasets(source3, target3)
        self.assertEqual(actual_count3, 1)
        self.assertEqual(actual_results3, (source_dataset1_2, ))

        source4 = [source_dataset1_1, source_dataset1_2]
        target4 = []
        actual_count4, actual_results4 = DatasetAction.get_destroy_datasets(source4, target4)
        self.assertEqual(actual_count4, 2)
        self.assertEqual(set(actual_results4), {source_dataset1_1, source_dataset1_2, })

        source5 = []
        target5 = [target_dataset1_1, target_dataset1_2]
        actual_count5, actual_results5 = DatasetAction.get_destroy_datasets(source5, target5)
        self.assertEqual(actual_count5, 0)
        self.assertEqual(actual_results5, ())

    def test_get_intersection_datasets(self):
        source_dataset1_1 = BigQueryDataset(
            dataset_id='test1',
            friendly_name='test_friendly_name',
            description='test_description',
            default_table_expiration_ms=24 * 60 * 60 * 1000,
            location='US'
        )
        source_dataset1_2 = BigQueryDataset(
            dataset_id='test2',
            friendly_name='foo_bar',
            description='fizz_buzz',
            default_table_expiration_ms=60 * 60 * 1000,
            location='EU'
        )

        target_dataset1_1 = BigQueryDataset(
            dataset_id='test1',
            friendly_name='test_friendly_name',
            description='test_description',
            default_table_expiration_ms=24 * 60 * 60 * 1000,
            location='US'
        )
        target_dataset1_2 = BigQueryDataset(
            dataset_id='test2',
            friendly_name='foo_bar',
            description='fizz_buzz',
            default_table_expiration_ms=60 * 60 * 1000,
            location='EU'
        )

        source1 = [source_dataset1_1]
        target1 = [target_dataset1_1, target_dataset1_2]
        actual_count1, actual_results1 = DatasetAction.get_intersection_datasets(source1, target1)
        self.assertEqual(actual_count1, 1)
        self.assertEqual(actual_results1, (source_dataset1_1, ))

        source2 = [source_dataset1_1, source_dataset1_2]
        target2 = [target_dataset1_1, target_dataset1_2]
        actual_count2, actual_results2 = DatasetAction.get_intersection_datasets(source2, target2)
        self.assertEqual(actual_count2, 2)
        self.assertEqual(set(actual_results2), {source_dataset1_1, source_dataset1_2})

        source3 = [source_dataset1_1, source_dataset1_2]
        target3 = [target_dataset1_1]
        actual_count3, actual_results3 = DatasetAction.get_intersection_datasets(source3, target3)
        self.assertEqual(actual_count3, 1)
        self.assertEqual(actual_results3, (source_dataset1_1, ))

        source4 = [source_dataset1_1, source_dataset1_2]
        target4 = []
        actual_count4, actual_results4 = DatasetAction.get_intersection_datasets(source4, target4)
        self.assertEqual(actual_count4, 0)
        self.assertEqual(actual_results4, ())

        source5 = []
        target5 = [target_dataset1_1, target_dataset1_2]
        actual_count5, actual_results5 = DatasetAction.get_intersection_datasets(source5, target5)
        self.assertEqual(actual_count5, 0)
        self.assertEqual(actual_results5, ())

    # TODO
    # test_plan_add
    # test_add
    # test_plan_change
    # test_change
    # test_plan_destroy
    # test_destroy
    # test_plan_intersection_destroy
    # test_intersection_destroy

    # TODO
    # test_list_datasets
    # test_export
