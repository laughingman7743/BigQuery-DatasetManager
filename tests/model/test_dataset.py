# -*- coding: utf-8 -*-
from __future__ import absolute_import

import unittest

from google.cloud.bigquery import AccessEntry, Dataset

from bqdm.model.dataset import BigQueryAccessEntry, BigQueryDataset
from tests.util import with_client


class TestBigQueryAccessEntry(unittest.TestCase):

    def test_eq(self):
        access_entry1_1 = BigQueryAccessEntry(
            'OWNER',
            'specialGroup',
            'projectOwners'
        )
        access_entry1_2 = BigQueryAccessEntry(
            'OWNER',
            'specialGroup',
            'projectOwners'
        )
        self.assertEqual(access_entry1_1, access_entry1_2)

        access_entry2_1 = BigQueryAccessEntry(
            'READER',
            'specialGroup',
            'projectReaders'
        )
        access_entry2_2 = BigQueryAccessEntry(
            'READER',
            'userByEmail',
            'a@b.c'
        )
        self.assertNotEqual(access_entry2_1, access_entry2_2)

        access_entry3_1 = BigQueryAccessEntry(
            'WRITER',
            'specialGroup',
            'projectWriters'
        )
        access_entry3_2 = BigQueryAccessEntry(
            None,
            'view',
            {
                'datasetId': 'test',
                'projectId': 'test-project',
                'tableId': 'test_table'
            }
        )
        access_entry3_3 = BigQueryAccessEntry(
            None,
            'view',
            {
                'datasetId': 'test',
                'projectId': 'test-project',
                'tableId': 'foo_bar'
            }
        )
        self.assertNotEqual(access_entry3_1, access_entry3_2)
        self.assertNotEqual(access_entry3_2, access_entry3_3)

        access_entry4_1 = BigQueryAccessEntry(
            None,
            'view',
            {
                'datasetId': 'test',
                'projectId': 'test-project',
                'tableId': 'test_table'
            }
        )
        access_entry4_2 = BigQueryAccessEntry(
            None,
            'view',
            {
                'datasetId': 'test',
                'projectId': 'test-project',
                'tableId': 'test_table'
            }
        )
        self.assertEqual(access_entry4_1, access_entry4_2)

    def test_from_dict(self):
        expected_access_entry1 = BigQueryAccessEntry(
            'OWNER',
            'specialGroup',
            'projectOwners'
        )
        actual_access_entry1_1 = BigQueryAccessEntry.from_dict({
            'role': 'OWNER',
            'entity_type': 'specialGroup',
            'entity_id': 'projectOwners'
        })
        self.assertEqual(expected_access_entry1, actual_access_entry1_1)
        actual_access_entry1_2 = BigQueryAccessEntry.from_dict({
            'role': 'WRITER',
            'entity_type': 'specialGroup',
            'entity_id': 'projectWriters'
        })
        self.assertNotEqual(expected_access_entry1, actual_access_entry1_2)

        expected_access_entry2 = BigQueryAccessEntry(
            None,
            'view',
            {
                'datasetId': 'test',
                'projectId': 'test-project',
                'tableId': 'test_table'
            }
        )
        actual_access_entry2_1 = BigQueryAccessEntry.from_dict({
            'role': None,
            'entity_type': 'view',
            'entity_id': {
                'datasetId': 'test',
                'projectId': 'test-project',
                'tableId': 'test_table'
            }
        })
        self.assertEqual(expected_access_entry2, actual_access_entry2_1)
        actual_access_entry2_2 = BigQueryAccessEntry.from_dict({
            'role': None,
            'entity_type': 'view',
            'entity_id': {
                'datasetId': 'test',
                'projectId': 'test-project',
                'tableId': 'foo_bar'
            }
        })
        self.assertNotEqual(expected_access_entry2, actual_access_entry2_2)

    def test_from_access_entry(self):
        expected_access_entry1 = BigQueryAccessEntry(
            'OWNER',
            'specialGroup',
            'projectOwners'
        )
        actual_access_entry1_1 = BigQueryAccessEntry.from_access_entry(
            AccessEntry('OWNER', 'specialGroup', 'projectOwners'))
        self.assertEqual(expected_access_entry1, actual_access_entry1_1)
        actual_access_entry1_2 = BigQueryAccessEntry.from_dict(
            AccessEntry('WRITER', 'specialGroup', 'projectWriters'))
        self.assertNotEqual(expected_access_entry1, actual_access_entry1_2)

        expected_access_entry2 = BigQueryAccessEntry(
            None,
            'view',
            {
                'datasetId': 'test',
                'projectId': 'test-project',
                'tableId': 'test_table'
            }
        )
        actual_access_entry2_1 = BigQueryAccessEntry.from_access_entry(
            AccessEntry(None, 'view', {
                'datasetId': 'test',
                'projectId': 'test-project',
                'tableId': 'test_table'
            })
        )
        self.assertEqual(expected_access_entry2, actual_access_entry2_1)
        actual_access_entry2_2 = BigQueryAccessEntry.from_access_entry(
            AccessEntry(None, 'view', {
                'datasetId': 'test',
                'projectId': 'test-project',
                'tableId': 'foo_bar'
            })
        )
        self.assertNotEqual(expected_access_entry2, actual_access_entry2_2)

    def test_to_access_entry(self):
        expected_access_entry1 = AccessEntry(
            'OWNER',
            'specialGroup',
            'projectOwners'
        )
        actual_access_entry1_1 = BigQueryAccessEntry.to_access_entry(
            BigQueryAccessEntry('OWNER', 'specialGroup', 'projectOwners'))
        self.assertEqual(expected_access_entry1, actual_access_entry1_1)
        actual_access_entry1_2 = BigQueryAccessEntry.to_access_entry(
            BigQueryAccessEntry('WRITER', 'specialGroup', 'projectWriters'))
        self.assertNotEqual(expected_access_entry1, actual_access_entry1_2)

        expected_access_entry2 = AccessEntry(
            None,
            'view',
            {
                'datasetId': 'test',
                'projectId': 'test-project',
                'tableId': 'test_table'
            }
        )
        actual_access_entry2_1 = BigQueryAccessEntry.to_access_entry(
            BigQueryAccessEntry(None, 'view', {
                'datasetId': 'test',
                'projectId': 'test-project',
                'tableId': 'test_table'
            })
        )
        self.assertEqual(expected_access_entry2, actual_access_entry2_1)
        actual_access_entry2_2 = BigQueryAccessEntry.to_access_entry(
            BigQueryAccessEntry(None, 'view', {
                'datasetId': 'test',
                'projectId': 'test-project',
                'tableId': 'foo_bar'
            })
        )
        self.assertNotEqual(expected_access_entry2, actual_access_entry2_2)


class TestBigQueryDataset(unittest.TestCase):

    def test_eq(self):
        dataset1_1 = BigQueryDataset(
            'test',
            'test_friendly_name',
            'test_description',
            24 * 60 * 60 * 1000,
            'US',
            None,
            None
        )
        dataset1_2 = BigQueryDataset(
            'test',
            'test_friendly_name',
            'test_description',
            24 * 60 * 60 * 1000,
            'US',
            None,
            None
        )
        self.assertEqual(dataset1_1, dataset1_2)

        dataset2_1 = BigQueryDataset(
            'test',
            'test_friendly_name',
            'test_description',
            24 * 60 * 60 * 1000,
            'US',
            None,
            None
        )
        dataset2_2 = BigQueryDataset(
            'test',
            'fizz_buzz',
            'foo_bar',
            60 * 60 * 1000,
            'EU',
            None,
            None
        )
        self.assertNotEqual(dataset2_1, dataset2_2)

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

        dataset_with_access_entry1_1 = BigQueryDataset(
            'test',
            'test_friendly_name',
            'test_description',
            24 * 60 * 60 * 1000,
            'US',
            None,
            [access_entry1]
        )
        dataset_with_access_entry1_2 = BigQueryDataset(
            'test',
            'test_friendly_name',
            'test_description',
            24 * 60 * 60 * 1000,
            'US',
            None,
            [access_entry1]
        )
        self.assertEqual(dataset_with_access_entry1_1, dataset_with_access_entry1_2)

        dataset_with_access_entry2_1 = BigQueryDataset(
            'test',
            'test_friendly_name',
            'test_description',
            24 * 60 * 60 * 1000,
            'US',
            None,
            [access_entry1]
        )
        dataset_with_access_entry2_2 = BigQueryDataset(
            'foo',
            'bar',
            'test_description',
            24 * 60 * 60 * 1000,
            'US',
            None,
            [access_entry2]
        )
        self.assertNotEqual(dataset_with_access_entry2_1, dataset_with_access_entry2_2)

        dataset_with_access_entry3_1 = BigQueryDataset(
            'foo',
            'bar',
            'test_description',
            24 * 60 * 60 * 1000,
            'US',
            None,
            [access_entry1, access_entry2]
        )
        dataset_with_access_entry3_2 = BigQueryDataset(
            'foo',
            'bar',
            'test_description',
            24 * 60 * 60 * 1000,
            'US',
            None,
            [access_entry2, access_entry1]
        )
        dataset_with_access_entry3_3 = BigQueryDataset(
            'foo',
            'bar',
            'test_description',
            24 * 60 * 60 * 1000,
            'US',
            None,
            (access_entry1, access_entry2)
        )
        self.assertEqual(dataset_with_access_entry3_1, dataset_with_access_entry3_2)
        self.assertEqual(dataset_with_access_entry3_1, dataset_with_access_entry3_3)
        self.assertEqual(dataset_with_access_entry3_2, dataset_with_access_entry3_3)

        dataset_with_access_entry4_1 = BigQueryDataset(
            'foo',
            'bar',
            'test_description',
            24 * 60 * 60 * 1000,
            'US',
            None,
            [access_entry1, access_entry2]
        )
        dataset_with_access_entry4_2 = BigQueryDataset(
            'foo',
            'bar',
            'test_description',
            24 * 60 * 60 * 1000,
            'US',
            None,
            [access_entry3]
        )
        dataset_with_access_entry4_3 = BigQueryDataset(
            'foo',
            'bar',
            'test_description',
            24 * 60 * 60 * 1000,
            'US',
            None,
            [access_entry1, access_entry3]
        )
        self.assertNotEqual(dataset_with_access_entry4_1, dataset_with_access_entry4_2)
        self.assertNotEqual(dataset_with_access_entry4_1, dataset_with_access_entry4_3)
        self.assertNotEqual(dataset_with_access_entry4_2, dataset_with_access_entry4_3)

        label1 = {
            'foo': 'bar'
        }
        label2 = {
            'foo': 'bar'
        }

        dataset_with_label5_1 = BigQueryDataset(
            'foo',
            'bar',
            'test_description',
            24 * 60 * 60 * 1000,
            'US',
            label1,
            None
        )
        dataset_with_label5_2 = BigQueryDataset(
            'foo',
            'bar',
            'test_description',
            24 * 60 * 60 * 1000,
            'US',
            label1,
            None
        )
        self.assertEqual(dataset_with_label5_1, dataset_with_label5_2)

        dataset_with_label6_1 = BigQueryDataset(
            'foo',
            'bar',
            'test_description',
            24 * 60 * 60 * 1000,
            'US',
            label1,
            None
        )
        dataset_with_label6_2 = BigQueryDataset(
            'foo',
            'bar',
            'test_description',
            24 * 60 * 60 * 1000,
            'US',
            label2,
            None
        )
        self.assertNotEqual(dataset_with_label6_1, dataset_with_label6_2)

    def test_from_dict(self):
        expected_dataset1 = BigQueryDataset(
            'test',
            'test_friendly_name',
            'test_description',
            24 * 60 * 60 * 1000,
            'US',
            None,
            None
        )
        actual_dataset1_1 = BigQueryDataset.from_dict({
            'dataset_id': 'test',
            'friendly_name': 'test_friendly_name',
            'description': 'test_description',
            'default_table_expiration_ms': 24 * 60 * 60 * 1000,
            'location': 'US',
            'labels': None,
            'access_entries': None
        })
        self.assertEqual(expected_dataset1, actual_dataset1_1)
        actual_dataset1_2 = BigQueryDataset.from_dict({
            'dataset_id': 'test',
            'friendly_name': 'foo_bar',
            'description': 'fizz_buzz',
            'default_table_expiration_ms': 60 * 60 * 1000,
            'location': 'EU',
            'labels': None,
            'access_entries': None
        })
        self.assertNotEqual(expected_dataset1, actual_dataset1_2)

        expected_dataset2 = BigQueryDataset(
            'test',
            'test_friendly_name',
            'test_description',
            24 * 60 * 60 * 1000,
            'US',
            None,
            [
                BigQueryAccessEntry(
                    'OWNER',
                    'specialGroup',
                    'projectOwners'
                )
            ]
        )
        actual_dataset2_1 = BigQueryDataset.from_dict({
            'dataset_id': 'test',
            'friendly_name': 'test_friendly_name',
            'description': 'test_description',
            'default_table_expiration_ms': 24 * 60 * 60 * 1000,
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
        self.assertEqual(expected_dataset2, actual_dataset2_1)
        actual_dataset2_2 = BigQueryDataset.from_dict({
            'dataset_id': 'test',
            'friendly_name': 'test_friendly_name',
            'description': 'test_description',
            'default_table_expiration_ms': 24 * 60 * 60 * 1000,
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
        self.assertNotEqual(expected_dataset2, actual_dataset2_2)

        expected_dataset3 = BigQueryDataset(
            'test',
            'test_friendly_name',
            'test_description',
            24 * 60 * 60 * 1000,
            'US',
            {
                'foo': 'bar'
            },
            None
        )
        actual_dataset3_1 = BigQueryDataset.from_dict({
            'dataset_id': 'test',
            'friendly_name': 'test_friendly_name',
            'description': 'test_description',
            'default_table_expiration_ms': 24 * 60 * 60 * 1000,
            'location': 'US',
            'labels': {
                'foo': 'bar'
            },
            'access_entries': None
        })
        self.assertEqual(expected_dataset3, actual_dataset3_1)
        actual_dataset3_2 = BigQueryDataset.from_dict({
            'dataset_id': 'test',
            'friendly_name': 'test_friendly_name',
            'description': 'test_description',
            'default_table_expiration_ms': 24 * 60 * 60 * 1000,
            'location': 'US',
            'labels': {
                'foo': 'bar',
                'fizz': 'buzz'
            },
            'access_entries': None
        })
        self.assertNotEqual(expected_dataset3, actual_dataset3_2)

    @with_client
    def test_from_dataset(self, client):
        expected_dataset1 = BigQueryDataset(
            'test',
            'test_friendly_name',
            'test_description',
            24 * 60 * 60 * 1000,
            'US',
            None,
            None
        )
        dataset_ref1_1 = client.dataset('test')
        dataset1_1 = Dataset(dataset_ref1_1)
        dataset1_1.friendly_name = 'test_friendly_name'
        dataset1_1.description = 'test_description'
        dataset1_1.default_table_expiration_ms = 24 * 60 * 60 * 1000
        dataset1_1.location = 'US'
        actual_dataset1_1 = BigQueryDataset.from_dataset(dataset1_1)
        self.assertEqual(expected_dataset1, actual_dataset1_1)
        dataset_ref1_2 = client.dataset('test')
        dataset1_2 = Dataset(dataset_ref1_2)
        dataset1_2.friendly_name = 'foo_bar'
        dataset1_2.description = 'fizz_buzz'
        dataset1_2.default_table_expiration_ms = 60 * 60 * 1000
        dataset1_2.location = 'EU'
        actual_dataset1_2 = BigQueryDataset.from_dataset(dataset1_2)
        self.assertNotEqual(expected_dataset1, actual_dataset1_2)

        expected_dataset2 = BigQueryDataset(
            'test',
            'test_friendly_name',
            'test_description',
            24 * 60 * 60 * 1000,
            'US',
            None,
            [
                BigQueryAccessEntry(
                    'OWNER',
                    'specialGroup',
                    'projectOwners'
                )
            ]
        )
        dataset_ref2_1 = client.dataset('test')
        dataset2_1 = Dataset(dataset_ref2_1)
        dataset2_1.friendly_name = 'test_friendly_name'
        dataset2_1.description = 'test_description'
        dataset2_1.default_table_expiration_ms = 24 * 60 * 60 * 1000
        dataset2_1.location = 'US'
        dataset2_1.access_entries = [AccessEntry('OWNER', 'specialGroup', 'projectOwners')]
        actual_dataset2_1 = BigQueryDataset.from_dataset(dataset2_1)
        self.assertEqual(expected_dataset2, actual_dataset2_1)
        dataset_ref2_2 = client.dataset('test')
        dataset2_2 = Dataset(dataset_ref2_2)
        dataset2_2.friendly_name = 'test_friendly_name'
        dataset2_2.description = 'test_description'
        dataset2_2.default_table_expiration_ms = 24 * 60 * 60 * 1000
        dataset2_2.location = 'US'
        dataset2_2.access_entries =  [AccessEntry(None, 'view', {
            'datasetId': 'test',
            'projectId': 'test-project',
            'tableId': 'test_table'
        })]
        actual_dataset2_2 = BigQueryDataset.from_dataset(dataset2_2)
        self.assertNotEqual(expected_dataset2, actual_dataset2_2)

        expected_dataset3 = BigQueryDataset(
            'test',
            'test_friendly_name',
            'test_description',
            24 * 60 * 60 * 1000,
            'US',
            {
                'foo': 'bar'
            },
            None
        )
        dataset_ref3_1 = client.dataset('test')
        dataset3_1 = Dataset(dataset_ref3_1)
        dataset3_1.friendly_name = 'test_friendly_name'
        dataset3_1.description = 'test_description'
        dataset3_1.default_table_expiration_ms = 24 * 60 * 60 * 1000
        dataset3_1.location = 'US'
        dataset3_1.labels = {
            'foo': 'bar'
        }
        actual_dataset3_1 = BigQueryDataset.from_dataset(dataset3_1)
        self.assertEqual(expected_dataset3, actual_dataset3_1)
        dataset_ref3_2 = client.dataset('test')
        dataset3_2 = Dataset(dataset_ref3_2)
        dataset3_2.friendly_name = 'test_friendly_name'
        dataset3_2.description = 'test_description'
        dataset3_2.default_table_expiration_ms = 24 * 60 * 60 * 1000
        dataset3_2.location = 'US'
        dataset3_2.labels = {
            'foo': 'bar',
            'fizz': 'buzz'
        }
        actual_dataset3_2 = BigQueryDataset.from_dataset(dataset3_2)
        self.assertNotEqual(expected_dataset3, actual_dataset3_2)

    @with_client
    def test_to_dataset(self, client):
        dataset_ref1 = client.dataset('test')
        expected_dataset1 = Dataset(dataset_ref1)
        expected_dataset1.friendly_name = 'test_friendly_name'
        expected_dataset1.description = 'test_description'
        expected_dataset1.default_table_expiration_ms = 24 * 60 * 60 * 1000
        expected_dataset1.location = 'US'
        actual_dataset1_1 = BigQueryDataset.to_dataset(client, BigQueryDataset(
            'test',
            'test_friendly_name',
            'test_description',
            24 * 60 * 60 * 1000,
            'US',
            None,
            None
        ))
        self.assertEqual(expected_dataset1.dataset_id, actual_dataset1_1.dataset_id)
        self.assertEqual(expected_dataset1.friendly_name, actual_dataset1_1.friendly_name)
        self.assertEqual(expected_dataset1.description, actual_dataset1_1.description)
        self.assertEqual(expected_dataset1.default_table_expiration_ms,
                         actual_dataset1_1.default_table_expiration_ms)
        self.assertEqual(expected_dataset1.location, actual_dataset1_1.location)
        self.assertEqual(expected_dataset1.labels, actual_dataset1_1.labels)
        self.assertEqual(expected_dataset1.access_entries, actual_dataset1_1.access_entries)
        actual_dataset1_2 = BigQueryDataset.to_dataset(client, BigQueryDataset(
            'aaa',
            'foo_bar',
            'fizz_buzz',
            60 * 60 * 1000,
            'EU',
            None,
            None
        ))
        self.assertNotEqual(expected_dataset1.dataset_id, actual_dataset1_2.dataset_id)
        self.assertNotEqual(expected_dataset1.friendly_name, actual_dataset1_2.friendly_name)
        self.assertNotEqual(expected_dataset1.description, actual_dataset1_2.description)
        self.assertNotEqual(expected_dataset1.default_table_expiration_ms,
                            actual_dataset1_2.default_table_expiration_ms)
        self.assertNotEqual(expected_dataset1.location, actual_dataset1_2.location)
        self.assertEqual(expected_dataset1.labels, actual_dataset1_2.labels)
        self.assertEqual(expected_dataset1.access_entries, actual_dataset1_2.access_entries)

        dataset_ref2 = client.dataset('test')
        expected_dataset2 = Dataset(dataset_ref2)
        expected_dataset2.friendly_name = 'test_friendly_name'
        expected_dataset2.description = 'test_description'
        expected_dataset2.default_table_expiration_ms = 24 * 60 * 60 * 1000
        expected_dataset2.location = 'US'
        expected_dataset2.access_entries = (
            AccessEntry(
                'OWNER',
                'specialGroup',
                'projectOwners'
            ),
        )
        actual_dataset2_1 = BigQueryDataset.to_dataset(client, BigQueryDataset(
            'test',
            'test_friendly_name',
            'test_description',
            24 * 60 * 60 * 1000,
            'US',
            None,
            [
                BigQueryAccessEntry('OWNER', 'specialGroup', 'projectOwners')
            ]
        ))
        self.assertEqual(expected_dataset2.access_entries, actual_dataset2_1.access_entries)
        actual_dataset2_2 = BigQueryDataset.to_dataset(client, BigQueryDataset(
            'test',
            'test_friendly_name',
            'test_description',
            24 * 60 * 60 * 1000,
            'US',
            None,
            [
                BigQueryAccessEntry(None, 'view', {
                    'datasetId': 'test',
                    'projectId': 'test-project',
                    'tableId': 'test_table'
                })
            ]
        ))
        self.assertNotEqual(expected_dataset2.access_entries, actual_dataset2_2.access_entries)

        dataset_ref3 = client.dataset('test')
        expected_dataset3 = Dataset(dataset_ref3)
        expected_dataset3.friendly_name = 'test_friendly_name'
        expected_dataset3.description = 'test_description'
        expected_dataset3.default_table_expiration_ms = 24 * 60 * 60 * 1000
        expected_dataset3.location = 'US'
        expected_dataset3.labels = {
            'foo': 'bar'
        }
        actual_dataset3_1 = BigQueryDataset.to_dataset(BigQueryDataset(
            'test',
            'test_friendly_name',
            'test_description',
            24 * 60 * 60 * 1000,
            'US',
            {
                'foo': 'bar'
            },
            None
        ))
        self.assertEqual(expected_dataset3.labels, actual_dataset3_1.labels)
        actual_dataset3_2 = BigQueryDataset.to_dataset(BigQueryDataset(
            'test',
            'test_friendly_name',
            'test_description',
            24 * 60 * 60 * 1000,
            'US',
            {
                'foo': 'bar',
                'fizz': 'buzz'
            },
            None
        ))
        self.assertNotEqual(expected_dataset3.labels, actual_dataset3_2.labels)
