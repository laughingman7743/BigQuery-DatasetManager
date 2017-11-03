# -*- coding: utf-8 -*-
import unittest

from bqdm.action import DatasetAction
from bqdm.model import BigQueryDataset, BigQueryAccessEntry


class TestAction(unittest.TestCase):

    def test_get_add_datasets(self):
        dataset1 = BigQueryDataset(
            'test1',
            'test_friendly_name',
            'test_description',
            24 * 30 * 60 * 1000,
            'US',
            None,
            None
        )
        dataset2 = BigQueryDataset(
            'test2',
            'test_friendly_name',
            'test_description',
            24 * 30 * 60 * 1000,
            'US',
            None,
            None
        )

        source1 = [dataset1]
        target1 = [dataset1, dataset2]
        actual_count1, actual_results1 = DatasetAction.get_add_datasets(source1, target1)
        self.assertEqual(actual_count1, 1)
        self.assertEqual(actual_results1, tuple([dataset2]))

        source2 = [dataset1, dataset2]
        target2 = [dataset1, dataset2]
        actual_count2, actual_results2 = DatasetAction.get_add_datasets(source2, target2)
        self.assertEqual(actual_count2, 0)
        self.assertEqual(actual_results2, tuple())

        source3 = [dataset1, dataset2]
        target3 = [dataset1]
        actual_count3, actual_results3 = DatasetAction.get_add_datasets(source3, target3)
        self.assertEqual(actual_count3, 0)
        self.assertEqual(actual_results3, tuple())

    def test_get_change_datasets(self):
        dataset1_1 = BigQueryDataset(
            'test1',
            'test_friendly_name',
            'test_description',
            24 * 30 * 60 * 1000,
            'US',
            None,
            None
        )
        dataset1_2 = BigQueryDataset(
            'test1',
            'foo',
            'bar',
            None,
            'UK',
            None,
            None
        )
        dataset2_1 = BigQueryDataset(
            'test2',
            'test_friendly_name',
            'test_description',
            24 * 30 * 60 * 1000,
            'US',
            None,
            None
        )
        dataset2_2 = BigQueryDataset(
            'test2',
            'test_friendly_name',
            'test_description',
            24 * 30 * 60 * 1000,
            'US',
            None,
            [
                BigQueryAccessEntry(
                    None,
                    'view',
                    {
                        'datasetId': 'test',
                        'projectId': 'test-project',
                        'tableId': 'test_table'
                    }
                )
            ]
        )

        source1 = [dataset1_1]
        target1 = [dataset1_2, dataset2_1]
        actual_count1, actual_results1 = DatasetAction.get_change_datasets(source1, target1)
        self.assertEqual(actual_count1, 1)
        self.assertEqual(actual_results1, tuple([dataset1_2]))

        source2 = [dataset1_1, dataset2_1]
        target2 = [dataset1_2, dataset2_1]
        actual_count2, actual_results2 = DatasetAction.get_change_datasets(source2, target2)
        self.assertEqual(actual_count2, 1)
        self.assertEqual(actual_results2, tuple([dataset1_2]))

        source3 = [dataset1_1, dataset2_1]
        target3 = [dataset1_2, dataset2_2]
        actual_count3, actual_results3 = DatasetAction.get_change_datasets(source3, target3)
        self.assertEqual(actual_count3, 2)
        self.assertEqual(set(actual_results3), {dataset1_2, dataset2_2})

        source4 = [dataset1_2, dataset2_2]
        target4 = [dataset1_2]
        actual_count4, actual_results4 = DatasetAction.get_change_datasets(source4, target4)
        self.assertEqual(actual_count4, 0)
        self.assertEqual(actual_results4, tuple())

        source5 = [dataset1_2, dataset2_2]
        target5 = [dataset1_2, dataset2_2]
        actual_count5, actual_results5 = DatasetAction.get_change_datasets(source5, target5)
        self.assertEqual(actual_count5, 0)
        self.assertEqual(actual_results5, tuple())

    def test_get_destroy_datasets(self):
        dataset1_1 = BigQueryDataset(
            'test1',
            'test_friendly_name',
            'test_description',
            24 * 30 * 60 * 1000,
            'US',
            None,
            None
        )
        dataset1_2 = BigQueryDataset(
            'test1',
            'foo',
            'bar',
            None,
            'UK',
            None,
            None
        )
        dataset2_1 = BigQueryDataset(
            'test2',
            'test_friendly_name',
            'test_description',
            24 * 30 * 60 * 1000,
            'US',
            None,
            None
        )
        dataset2_2 = BigQueryDataset(
            'test2',
            'test_friendly_name',
            'test_description',
            24 * 30 * 60 * 1000,
            'US',
            None,
            [
                BigQueryAccessEntry(
                    None,
                    'view',
                    {
                        'datasetId': 'test',
                        'projectId': 'test-project',
                        'tableId': 'test_table'
                    }
                )
            ]
        )

        source1 = [dataset1_1]
        target1 = [dataset1_2, dataset2_1]
        actual_count1, actual_results1 = DatasetAction.get_destroy_datasets(source1, target1)
        self.assertEqual(actual_count1, 0)
        self.assertEqual(actual_results1, tuple())

        source2 = [dataset1_1, dataset2_1]
        target2 = [dataset1_2, dataset2_1]
        actual_count2, actual_results2 = DatasetAction.get_destroy_datasets(source2, target2)
        self.assertEqual(actual_count2, 0)
        self.assertEqual(actual_results2, tuple())

        source3 = [dataset1_1, dataset2_1]
        target3 = [dataset1_2, dataset2_2]
        actual_count3, actual_results3 = DatasetAction.get_destroy_datasets(source3, target3)
        self.assertEqual(actual_count3, 0)
        self.assertEqual(actual_results3, tuple())

        source4 = [dataset1_2, dataset2_2]
        target4 = [dataset1_2]
        actual_count4, actual_results4 = DatasetAction.get_destroy_datasets(source4, target4)
        self.assertEqual(actual_count4, 1)
        self.assertEqual(actual_results4, tuple([dataset2_2]))

        source5 = [dataset1_2, dataset2_2]
        target5 = [dataset1_2, dataset2_2]
        actual_count5, actual_results5 = DatasetAction.get_destroy_datasets(source5, target5)
        self.assertEqual(actual_count5, 0)
        self.assertEqual(actual_results5, tuple())

    def test_get_intersection_datasets(self):
        dataset1_1 = BigQueryDataset(
            'test1',
            'test_friendly_name',
            'test_description',
            24 * 30 * 60 * 1000,
            'US',
            None,
            None
        )
        dataset1_2 = BigQueryDataset(
            'test1',
            'foo',
            'bar',
            None,
            'UK',
            None,
            None
        )
        dataset2_1 = BigQueryDataset(
            'test2',
            'test_friendly_name',
            'test_description',
            24 * 30 * 60 * 1000,
            'US',
            None,
            None
        )
        dataset2_2 = BigQueryDataset(
            'test2',
            'test_friendly_name',
            'test_description',
            24 * 30 * 60 * 1000,
            'US',
            None,
            [
                BigQueryAccessEntry(
                    None,
                    'view',
                    {
                        'datasetId': 'test',
                        'projectId': 'test-project',
                        'tableId': 'test_table'
                    }
                )
            ]
        )

        source1 = [dataset1_1]
        target1 = [dataset1_2, dataset2_1]
        actual_count1, actual_results1 = DatasetAction.get_intersection_datasets(source1, target1)
        self.assertEqual(actual_count1, 1)
        self.assertEqual(actual_results1, tuple([dataset1_1]))

        source2 = [dataset1_1, dataset2_1]
        target2 = [dataset1_2, dataset2_1]
        actual_count2, actual_results2 = DatasetAction.get_intersection_datasets(source2, target2)
        self.assertEqual(actual_count2, 2)
        self.assertEqual(set(actual_results2), {dataset1_1, dataset2_1})

        source3 = [dataset1_1, dataset2_1]
        target3 = [dataset1_2, dataset2_2]
        actual_count3, actual_results3 = DatasetAction.get_intersection_datasets(source3, target3)
        self.assertEqual(actual_count3, 2)
        self.assertEqual(set(actual_results3), {dataset1_1, dataset2_1})

        source4 = [dataset1_2, dataset2_2]
        target4 = [dataset1_2]
        actual_count4, actual_results4 = DatasetAction.get_intersection_datasets(source4, target4)
        self.assertEqual(actual_count4, 1)
        self.assertEqual(actual_results4, tuple([dataset1_2]))

        source5 = [dataset1_2, dataset2_2]
        target5 = [dataset1_2, dataset2_2]
        actual_count5, actual_results5 = DatasetAction.get_intersection_datasets(source5, target5)
        self.assertEqual(actual_count5, 2)
        self.assertEqual(set(actual_results5), {dataset1_2, dataset2_2})

        source6 = []
        target6 = [dataset1_2, dataset2_2]
        actual_count6, actual_results6 = DatasetAction.get_intersection_datasets(source6, target6)
        self.assertEqual(actual_count6, 0)
        self.assertEqual(actual_results6, tuple())
