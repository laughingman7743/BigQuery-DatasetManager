# -*- coding: utf-8 -*-
import unittest

from bqdm.model import BigQueryDataset, BigQueryAccessGrant
from bqdm.util import dump_dataset


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
