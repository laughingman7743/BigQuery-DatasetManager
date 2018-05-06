# -*- coding: utf-8 -*-
from __future__ import absolute_import

import unittest

from bqdm.model.dataset import BigQueryAccessEntry, BigQueryDataset
from bqdm.util import dump


class TestUtil(unittest.TestCase):

    def test_dump_dataset(self):
        dataset1 = BigQueryDataset(
            'test1',
            'test_friendly_name',
            'test_description',
            24 * 30 * 60 * 1000,
            'US',
            None,
            None
        )
        expected_dump_data1 = """dataset_id: test1
friendly_name: test_friendly_name
description: test_description
default_table_expiration_ms: 43200000
location: US
labels: null
access_entries: null
"""
        actual_dump_data1 = dump(dataset1)
        self.assertEqual(expected_dump_data1, actual_dump_data1)

        access_entry2 = BigQueryAccessEntry(
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
            None,
            [access_entry2]
        )
        expected_dump_data2 = """dataset_id: test2
friendly_name: test_friendly_name
description: test_description
default_table_expiration_ms: 43200000
location: US
labels: null
access_entries:
-   role: OWNER
    entity_type: specialGroup
    entity_id: projectOwners
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
            'test3',
            'test_friendly_name',
            'test_description',
            24 * 30 * 60 * 1000,
            'US',
            None,
            [access_entry3]
        )
        expected_dump_data3 = """dataset_id: test3
friendly_name: test_friendly_name
description: test_description
default_table_expiration_ms: 43200000
location: US
labels: null
access_entries:
-   role: null
    entity_type: view
    entity_id:
        datasetId: test
        projectId: test-project
        tableId: test_table
"""
        actual_dump_data3 = dump(dataset3)
        self.assertEqual(expected_dump_data3, actual_dump_data3)

        dataset4 = BigQueryDataset(
            'test4',
            'test_friendly_name',
            'test_description',
            24 * 30 * 60 * 1000,
            'US',
            None,
            [access_entry2, access_entry3]
        )
        expected_dump_data4 = """dataset_id: test4
friendly_name: test_friendly_name
description: test_description
default_table_expiration_ms: 43200000
location: US
labels: null
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
"""
        actual_dump_data4 = dump(dataset4)
        self.assertEqual(expected_dump_data4, actual_dump_data4)

        label5 = {
            'foo': 'bar'
        }
        dataset5 = BigQueryDataset(
            'test5',
            'test_friendly_name',
            'test_description',
            24 * 30 * 60 * 1000,
            'US',
            label5,
            None
        )
        expected_dump_data5 = """dataset_id: test5
friendly_name: test_friendly_name
description: test_description
default_table_expiration_ms: 43200000
location: US
labels:
    foo: bar
access_entries: null
"""
        actual_dump_data5 = dump(dataset5)
        self.assertEqual(expected_dump_data5, actual_dump_data5)

        label6 = {
            'aaa': 'bbb',
            'ccc': 'ddd'
        }
        dataset6 = BigQueryDataset(
            'test6',
            'test_friendly_name',
            'test_description',
            24 * 30 * 60 * 1000,
            'US',
            label6,
            None
        )
        expected_dump_data6 = """dataset_id: test6
friendly_name: test_friendly_name
description: test_description
default_table_expiration_ms: 43200000
location: US
labels:
    aaa: bbb
    ccc: ddd
access_entries: null
"""
        actual_dump_data6 = dump(dataset6)
        self.assertEqual(expected_dump_data6, actual_dump_data6)

    def test_dump_table(self):
        # TODO
        pass
