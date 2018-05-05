# -*- coding: utf-8 -*-
from __future__ import absolute_import
import unittest

from bqdm.dataset import BigQueryAccessEntry, BigQueryDataset


class TestBigQueryDataset(unittest.TestCase):

    def test_eq_dataset(self):
        dataset1 = BigQueryDataset(
            'test',
            'test_friendly_name',
            'test_description',
            24 * 30 * 60 * 1000,
            'US',
            None,
            None
        )
        dataset2 = BigQueryDataset(
            'test',
            'test_friendly_name',
            'test_description',
            24 * 30 * 60 * 1000,
            'US',
            None,
            None
        )
        dataset3 = BigQueryDataset(
            'test',
            'test_friendly_name',
            'test_description',
            24 * 30 * 60 * 1000,
            'EU',
            None,
            None
        )
        dataset4 = BigQueryDataset(
            'test',
            'test_friendly_name',
            'test_description',
            None,
            'EU',
            None,
            None
        )
        dataset5 = BigQueryDataset(
            'test',
            'foo_bar',
            'test_description',
            None,
            'EU',
            None,
            None
        )
        dataset6 = BigQueryDataset(
            'test',
            'foo_bar',
            'hoge_fuga',
            None,
            'EU',
            None,
            None
        )

        self.assertEqual(dataset1, dataset2)
        self.assertNotEqual(dataset1, dataset3)
        self.assertNotEqual(dataset1, dataset4)
        self.assertNotEqual(dataset3, dataset4)
        self.assertNotEqual(dataset4, dataset5)
        self.assertNotEqual(dataset5, dataset6)

    def test_eq_dataset_with_access_entry(self):
        access_entry1 = BigQueryAccessEntry(
            'OWNER',
            'specialGroup',
            'projectOwners'
        )
        access_entry2 = BigQueryAccessEntry(
            'OWNER',
            'specialGroup',
            'projectOwners'
        )
        access_entry3 = BigQueryAccessEntry(
            None,
            'view',
            {
                'datasetId': 'test',
                'projectId': 'test-project',
                'tableId': 'test_table'
            }
        )

        dataset_with_access_entry1 = BigQueryDataset(
            'test',
            'test_friendly_name',
            'test_description',
            24 * 30 * 60 * 1000,
            'US',
            None,
            [access_entry1]
        )
        dataset_with_access_entry2 = BigQueryDataset(
            'test',
            'test_friendly_name',
            'test_description',
            24 * 30 * 60 * 1000,
            'US',
            None,
            [access_entry2]
        )
        dataset_with_access_entry3 = BigQueryDataset(
            'test',
            'test_friendly_name',
            'test_description',
            24 * 30 * 60 * 1000,
            'US',
            None,
            [access_entry3]
        )
        dataset_with_access_entry4 = BigQueryDataset(
            'foo',
            'bar',
            'test_description',
            24 * 30 * 60 * 1000,
            'US',
            None,
            [access_entry1]
        )
        dataset_with_access_entry5 = BigQueryDataset(
            'foo',
            'bar',
            'test_description',
            24 * 30 * 60 * 1000,
            'US',
            None,
            [access_entry1, access_entry3]
        )
        dataset_with_access_entry6 = BigQueryDataset(
            'foo',
            'bar',
            'test_description',
            24 * 30 * 60 * 1000,
            'US',
            None,
            [access_entry3, access_entry1]
        )
        dataset_with_access_entry7 = BigQueryDataset(
            'foo',
            'bar',
            'test_description',
            24 * 30 * 60 * 1000,
            'US',
            None,
            [access_entry1, access_entry2]
        )

        self.assertEqual(dataset_with_access_entry1, dataset_with_access_entry2)
        self.assertNotEqual(dataset_with_access_entry1, dataset_with_access_entry3)
        self.assertNotEqual(dataset_with_access_entry1, dataset_with_access_entry4)
        self.assertNotEqual(dataset_with_access_entry3, dataset_with_access_entry4)
        self.assertEqual(dataset_with_access_entry5, dataset_with_access_entry6)
        self.assertEqual(dataset_with_access_entry4, dataset_with_access_entry7)
        self.assertNotEqual(dataset_with_access_entry6, dataset_with_access_entry7)

    def test_dataset_from_dict(self):
        dataset = BigQueryDataset(
            'test',
            'test_friendly_name',
            'test_description',
            24 * 30 * 60 * 1000,
            'US',
            None,
            None
        )

        dataset_from_dict1 = BigQueryDataset.from_dict({
            'dataset_id': 'test',
            'friendly_name': 'test_friendly_name',
            'description': 'test_description',
            'default_table_expiration_ms': 24 * 30 * 60 * 1000,
            'location': 'US',
            'labels': None,
            'access_entris': None
        })
        dataset_from_dict2 = BigQueryDataset.from_dict({
            'dataset_id': 'test',
            'friendly_name': 'test_friendly_name',
            'description': 'test_description',
            'default_table_expiration_ms': None,
            'location': 'US',
            'labels': None,
            'access_entries': None
        })
        dataset_from_dict3 = BigQueryDataset.from_dict({
            'dataset_id': 'foo',
            'friendly_name': 'bar',
            'description': 'test_description',
            'default_table_expiration_ms': None,
            'location': 'US',
            'labels': None,
            'access_entries': None
        })

        self.assertEqual(dataset, dataset_from_dict1)
        self.assertNotEqual(dataset, dataset_from_dict2)
        self.assertNotEqual(dataset, dataset_from_dict3)

    def test_dataset_from_dict_with_access_entry(self):
        access_entry1 = BigQueryAccessEntry(
            'OWNER',
            'specialGroup',
            'projectOwners'
        )
        access_entry2 = BigQueryAccessEntry(
            None,
            'view',
            {
                'datasetId': 'test',
                'projectId': 'test-project',
                'tableId': 'test_table'
            }
        )

        dataset_with_access_entry1 = BigQueryDataset(
            'test',
            'test_friendly_name',
            'test_description',
            24 * 30 * 60 * 1000,
            'US',
            None,
            [access_entry1]
        )
        dataset_with_access_entry2 = BigQueryDataset(
            'test',
            'test_friendly_name',
            'test_description',
            24 * 30 * 60 * 1000,
            'US',
            None,
            [access_entry2]
        )
        dataset_with_access_entry3 = BigQueryDataset(
            'test',
            'test_friendly_name',
            'test_description',
            24 * 30 * 60 * 1000,
            'US',
            None,
            [access_entry1, access_entry2]
        )
        dataset_with_access_entry4 = BigQueryDataset(
            'test',
            'test_friendly_name',
            'test_description',
            24 * 30 * 60 * 1000,
            'US',
            None,
            [access_entry1, access_entry1]
        )

        dataset_with_access_entry_from_dict1 = BigQueryDataset.from_dict({
            'dataset_id': 'test',
            'friendly_name': 'test_friendly_name',
            'description': 'test_description',
            'default_table_expiration_ms': 24 * 30 * 60 * 1000,
            'location': 'US',
            'labels': None,
            'access_entries': [
                {
                    'role': 'OWNER',
                    'entity_type': 'specialGroup',
                    'entity_id': 'projectOwners'
                }
            ]
        })
        dataset_with_access_entry_from_dict2 = BigQueryDataset.from_dict({
            'dataset_id': 'test',
            'friendly_name': 'test_friendly_name',
            'description': 'test_description',
            'default_table_expiration_ms': 24 * 30 * 60 * 1000,
            'location': 'US',
            'labels': None,
            'access_entries': [
                {
                    'role': None,
                    'entity_type': 'view',
                    'entity_id': {
                        'datasetId': 'test',
                        'projectId': 'test-project',
                        'tableId': 'test_table'
                    }
                }
            ]
        })
        dataset_with_access_entry_from_dict3 = BigQueryDataset.from_dict({
            'dataset_id': 'test',
            'friendly_name': 'test_friendly_name',
            'description': 'test_description',
            'default_table_expiration_ms': 24 * 30 * 60 * 1000,
            'location': 'US',
            'labels': None,
            'access_entries': [
                {
                    'role': 'OWNER',
                    'entity_type': 'specialGroup',
                    'entity_id': 'projectOwners'
                },
                {
                    'role': None,
                    'entity_type': 'view',
                    'entity_id': {
                        'datasetId': 'test',
                        'projectId': 'test-project',
                        'tableId': 'test_table'
                    }
                }
            ]
        })
        dataset_with_access_entry_from_dict4 = BigQueryDataset.from_dict({
            'dataset_id': 'test',
            'friendly_name': 'test_friendly_name',
            'description': 'test_description',
            'default_table_expiration_ms': 24 * 30 * 60 * 1000,
            'location': 'US',
            'labels': None,
            'access_entries': [
                {
                    'role': 'OWNER',
                    'entity_type': 'specialGroup',
                    'entity_id': 'projectOwners'
                },
                {
                    'role': 'OWNER',
                    'entity_type': 'specialGroup',
                    'entity_id': 'projectOwners'
                }
            ]
        })

        self.assertEqual(dataset_with_access_entry1, dataset_with_access_entry_from_dict1)
        self.assertNotEqual(dataset_with_access_entry1, dataset_with_access_entry_from_dict2)
        self.assertNotEqual(dataset_with_access_entry1, dataset_with_access_entry_from_dict3)
        self.assertEqual(dataset_with_access_entry2, dataset_with_access_entry_from_dict2)
        self.assertNotEqual(dataset_with_access_entry2, dataset_with_access_entry_from_dict1)
        self.assertNotEqual(dataset_with_access_entry2, dataset_with_access_entry_from_dict3)
        self.assertEqual(dataset_with_access_entry3, dataset_with_access_entry_from_dict3)
        self.assertNotEqual(dataset_with_access_entry3, dataset_with_access_entry_from_dict1)
        self.assertNotEqual(dataset_with_access_entry3, dataset_with_access_entry_from_dict2)
        self.assertEqual(dataset_with_access_entry4, dataset_with_access_entry_from_dict4)
        self.assertEqual(dataset_with_access_entry1, dataset_with_access_entry_from_dict4)
        self.assertEqual(dataset_with_access_entry4, dataset_with_access_entry_from_dict1)

    def test_dataset_from_dict_with_label(self):
        label1 = {
            'foo': 'bar'
        }
        label2 = {
            'aaa': 'bbb',
            'ccc': 'ddd'
        }

        dataset_with_label1 = BigQueryDataset(
            'test',
            'test_friendly_name',
            'test_description',
            24 * 30 * 60 * 1000,
            'US',
            label1,
            None
        )
        dataset_with_label2 = BigQueryDataset(
            'test',
            'test_friendly_name',
            'test_description',
            24 * 30 * 60 * 1000,
            'US',
            label2,
            None
        )

        dataset_with_label_from_dict1 = BigQueryDataset.from_dict({
            'dataset_id': 'test',
            'friendly_name': 'test_friendly_name',
            'description': 'test_description',
            'default_table_expiration_ms': 24 * 30 * 60 * 1000,
            'location': 'US',
            'labels': {
                'foo': 'bar'
            },
            'access_entries': None
        })
        dataset_with_label_from_dict2 = BigQueryDataset.from_dict({
            'dataset_id': 'test',
            'friendly_name': 'test_friendly_name',
            'description': 'test_description',
            'default_table_expiration_ms': 24 * 30 * 60 * 1000,
            'location': 'US',
            'labels': {
                'aaa': 'bbb',
                'ccc': 'ddd'
            },
            'access_entries': None
        })

        self.assertEqual(dataset_with_label1, dataset_with_label_from_dict1)
        self.assertNotEqual(dataset_with_label1, dataset_with_label_from_dict2)
        self.assertNotEqual(dataset_with_label2, dataset_with_label_from_dict1)
        self.assertEqual(dataset_with_label2, dataset_with_label_from_dict2)