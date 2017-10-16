# -*- coding: utf-8 -*-
import unittest

from bqdm.model import BigQueryDataset, BigQueryAccessGrant
from bqdm.util import (dump_dataset, get_add_datasets, get_change_datasets,
                       get_destroy_datasets, get_intersection_datasets)


class TestUtil(unittest.TestCase):

    def test_dump_dataset(self):
        dataset1 = BigQueryDataset(
            'test1',
            'test_friendly_name',
            'test_description',
            24 * 30 * 60 * 1000,
            'US',
            None
        )
        actual_dump_data1 = dump_dataset(dataset1)
        expected_dump_data1 = """name: test1
friendly_name: test_friendly_name
description: test_description
default_table_expiration_ms: 43200000
location: US
access_grants: null
"""
        self.assertEqual(actual_dump_data1, expected_dump_data1)

        access_grant2 = BigQueryAccessGrant(
            'OWNER',
            'specialGroup',
            'projectOwners'
        )
        dataset2 = BigQueryDataset(
            'test2',
            'test_friendly_name',
            'test_description',
            24 * 30 * 60 * 1000,
            'US',
            [access_grant2]
        )
        actual_dump_data2 = dump_dataset(dataset2)
        expected_dump_data2 = """name: test2
friendly_name: test_friendly_name
description: test_description
default_table_expiration_ms: 43200000
location: US
access_grants:
-   role: OWNER
    entity_type: specialGroup
    entity_id: projectOwners
"""
        self.assertEqual(actual_dump_data2, expected_dump_data2)

        access_grant3 = BigQueryAccessGrant(
            None,
            'view',
            {
                'datasetId': 'test',
                'projectId': 'test-project',
                'tableId': 'test_table'
            }
        )
        dataset3 = BigQueryDataset(
            'test3',
            'test_friendly_name',
            'test_description',
            24 * 30 * 60 * 1000,
            'US',
            [access_grant3]
        )
        actual_dump_data3 = dump_dataset(dataset3)
        expected_dump_data3 = """name: test3
friendly_name: test_friendly_name
description: test_description
default_table_expiration_ms: 43200000
location: US
access_grants:
-   role: null
    entity_type: view
    entity_id:
        datasetId: test
        projectId: test-project
        tableId: test_table
"""
        self.assertEqual(actual_dump_data3, expected_dump_data3)

        dataset4 = BigQueryDataset(
            'test4',
            'test_friendly_name',
            'test_description',
            24 * 30 * 60 * 1000,
            'US',
            [access_grant2, access_grant3]
        )
        actual_dump_data4 = dump_dataset(dataset4)
        expected_dump_data4 = """name: test4
friendly_name: test_friendly_name
description: test_description
default_table_expiration_ms: 43200000
location: US
access_grants:
-   role: OWNER
    entity_type: specialGroup
    entity_id: projectOwners
-   role: null
    entity_type: view
    entity_id:
        datasetId: test
        projectId: test-project
        tableId: test_table
"""
        self.assertEqual(actual_dump_data4, expected_dump_data4)

    def test_get_add_datasets(self):
        dataset1 = BigQueryDataset(
            'test1',
            'test_friendly_name',
            'test_description',
            24 * 30 * 60 * 1000,
            'US',
            None
        )
        dataset2 = BigQueryDataset(
            'test2',
            'test_friendly_name',
            'test_description',
            24 * 30 * 60 * 1000,
            'US',
            None
        )

        source1 = [dataset1]
        target1 = [dataset1, dataset2]
        actual_count1, actual_results1 = get_add_datasets(source1, target1)
        self.assertEqual(actual_count1, 1)
        self.assertEqual(actual_results1, tuple([dataset2]))

        source2 = [dataset1, dataset2]
        target2 = [dataset1, dataset2]
        actual_count2, actual_results2 = get_add_datasets(source2, target2)
        self.assertEqual(actual_count2, 0)
        self.assertEqual(actual_results2, tuple())

        source3 = [dataset1, dataset2]
        target3 = [dataset1]
        actual_count3, actual_results3 = get_add_datasets(source3, target3)
        self.assertEqual(actual_count3, 0)
        self.assertEqual(actual_results3, tuple())

    def test_get_change_datasets(self):
        dataset1_1 = BigQueryDataset(
            'test1',
            'test_friendly_name',
            'test_description',
            24 * 30 * 60 * 1000,
            'US',
            None
        )
        dataset1_2 = BigQueryDataset(
            'test1',
            'foo',
            'bar',
            None,
            'UK',
            None
        )
        dataset2_1 = BigQueryDataset(
            'test2',
            'test_friendly_name',
            'test_description',
            24 * 30 * 60 * 1000,
            'US',
            None
        )
        dataset2_2 = BigQueryDataset(
            'test2',
            'test_friendly_name',
            'test_description',
            24 * 30 * 60 * 1000,
            'US',
            [
                BigQueryAccessGrant(
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
        actual_count1, actual_results1 = get_change_datasets(source1, target1)
        self.assertEqual(actual_count1, 1)
        self.assertEqual(actual_results1, tuple([dataset1_2]))

        source2 = [dataset1_1, dataset2_1]
        target2 = [dataset1_2, dataset2_1]
        actual_count2, actual_results2 = get_change_datasets(source2, target2)
        self.assertEqual(actual_count2, 1)
        self.assertEqual(actual_results2, tuple([dataset1_2]))

        source3 = [dataset1_1, dataset2_1]
        target3 = [dataset1_2, dataset2_2]
        actual_count3, actual_results3 = get_change_datasets(source3, target3)
        self.assertEqual(actual_count3, 2)
        self.assertEqual(set(actual_results3), set([dataset1_2, dataset2_2]))

        source4 = [dataset1_2, dataset2_2]
        target4 = [dataset1_2]
        actual_count4, actual_results4 = get_change_datasets(source4, target4)
        self.assertEqual(actual_count4, 0)
        self.assertEqual(actual_results4, tuple())

        source5 = [dataset1_2, dataset2_2]
        target5 = [dataset1_2, dataset2_2]
        actual_count5, actual_results5 = get_change_datasets(source5, target5)
        self.assertEqual(actual_count5, 0)
        self.assertEqual(actual_results5, tuple())

    def test_get_destroy_datasets(self):
        dataset1_1 = BigQueryDataset(
            'test1',
            'test_friendly_name',
            'test_description',
            24 * 30 * 60 * 1000,
            'US',
            None
        )
        dataset1_2 = BigQueryDataset(
            'test1',
            'foo',
            'bar',
            None,
            'UK',
            None
        )
        dataset2_1 = BigQueryDataset(
            'test2',
            'test_friendly_name',
            'test_description',
            24 * 30 * 60 * 1000,
            'US',
            None
        )
        dataset2_2 = BigQueryDataset(
            'test2',
            'test_friendly_name',
            'test_description',
            24 * 30 * 60 * 1000,
            'US',
            [
                BigQueryAccessGrant(
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
        actual_count1, actual_results1 = get_destroy_datasets(source1, target1)
        self.assertEqual(actual_count1, 0)
        self.assertEqual(actual_results1, tuple())

        source2 = [dataset1_1, dataset2_1]
        target2 = [dataset1_2, dataset2_1]
        actual_count2, actual_results2 = get_destroy_datasets(source2, target2)
        self.assertEqual(actual_count2, 0)
        self.assertEqual(actual_results2, tuple())

        source3 = [dataset1_1, dataset2_1]
        target3 = [dataset1_2, dataset2_2]
        actual_count3, actual_results3 = get_destroy_datasets(source3, target3)
        self.assertEqual(actual_count3, 0)
        self.assertEqual(actual_results3, tuple())

        source4 = [dataset1_2, dataset2_2]
        target4 = [dataset1_2]
        actual_count4, actual_results4 = get_destroy_datasets(source4, target4)
        self.assertEqual(actual_count4, 1)
        self.assertEqual(actual_results4, tuple([dataset2_2]))

        source5 = [dataset1_2, dataset2_2]
        target5 = [dataset1_2, dataset2_2]
        actual_count5, actual_results5 = get_destroy_datasets(source5, target5)
        self.assertEqual(actual_count5, 0)
        self.assertEqual(actual_results5, tuple())

    def test_get_intersection_datasets(self):
        dataset1_1 = BigQueryDataset(
            'test1',
            'test_friendly_name',
            'test_description',
            24 * 30 * 60 * 1000,
            'US',
            None
        )
        dataset1_2 = BigQueryDataset(
            'test1',
            'foo',
            'bar',
            None,
            'UK',
            None
        )
        dataset2_1 = BigQueryDataset(
            'test2',
            'test_friendly_name',
            'test_description',
            24 * 30 * 60 * 1000,
            'US',
            None
        )
        dataset2_2 = BigQueryDataset(
            'test2',
            'test_friendly_name',
            'test_description',
            24 * 30 * 60 * 1000,
            'US',
            [
                BigQueryAccessGrant(
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
        actual_count1, actual_results1 = get_intersection_datasets(source1, target1)
        self.assertEqual(actual_count1, 1)
        self.assertEqual(actual_results1, tuple([dataset1_1]))

        source2 = [dataset1_1, dataset2_1]
        target2 = [dataset1_2, dataset2_1]
        actual_count2, actual_results2 = get_intersection_datasets(source2, target2)
        self.assertEqual(actual_count2, 2)
        self.assertEqual(set(actual_results2), set([dataset1_1, dataset2_1]))

        source3 = [dataset1_1, dataset2_1]
        target3 = [dataset1_2, dataset2_2]
        actual_count3, actual_results3 = get_intersection_datasets(source3, target3)
        self.assertEqual(actual_count3, 2)
        self.assertEqual(set(actual_results3), set([dataset1_1, dataset2_1]))

        source4 = [dataset1_2, dataset2_2]
        target4 = [dataset1_2]
        actual_count4, actual_results4 = get_intersection_datasets(source4, target4)
        self.assertEqual(actual_count4, 1)
        self.assertEqual(actual_results4, tuple([dataset1_2]))

        source5 = [dataset1_2, dataset2_2]
        target5 = [dataset1_2, dataset2_2]
        actual_count5, actual_results5 = get_intersection_datasets(source5, target5)
        self.assertEqual(actual_count5, 2)
        self.assertEqual(set(actual_results5), set([dataset1_2, dataset2_2]))

        source6 = []
        target6 = [dataset1_2, dataset2_2]
        actual_count6, actual_results6 = get_intersection_datasets(source6, target6)
        self.assertEqual(actual_count6, 0)
        self.assertEqual(actual_results6, tuple())
