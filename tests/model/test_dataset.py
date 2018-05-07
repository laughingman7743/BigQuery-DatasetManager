# -*- coding: utf-8 -*-
from __future__ import absolute_import

import unittest

from google.cloud.bigquery import AccessEntry

from bqdm.model.dataset import BigQueryAccessEntry, BigQueryDataset
from tests.util import make_dataset


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
        actual_access_entry1_2 = BigQueryAccessEntry.from_access_entry(
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
            dataset_id='test',
            friendly_name='test_friendly_name',
            description='test_description',
            default_table_expiration_ms=24 * 60 * 60 * 1000,
            location='US'
        )
        dataset1_2 = BigQueryDataset(
            dataset_id='test',
            friendly_name='test_friendly_name',
            description='test_description',
            default_table_expiration_ms=24 * 60 * 60 * 1000,
            location='US'
        )
        self.assertEqual(dataset1_1, dataset1_2)

        dataset2_1 = BigQueryDataset(
            dataset_id='test',
            friendly_name='test_friendly_name',
            description='test_description',
            default_table_expiration_ms=24 * 60 * 60 * 1000,
            location='US'
        )
        dataset2_2 = BigQueryDataset(
            dataset_id='test',
            friendly_name='fizz_buzz',
            description='foo_bar',
            default_table_expiration_ms=60 * 60 * 1000,
            location='EU'
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

        dataset3_1 = BigQueryDataset(
            dataset_id='test',
            friendly_name='test_friendly_name',
            description='test_description',
            default_table_expiration_ms=24 * 60 * 60 * 1000,
            location='US',
            access_entries=(access_entry1, )
        )
        dataset3_2 = BigQueryDataset(
            dataset_id='test',
            friendly_name='test_friendly_name',
            description='test_description',
            default_table_expiration_ms=24 * 60 * 60 * 1000,
            location='US',
            access_entries=(access_entry1, )
        )
        self.assertEqual(dataset3_1, dataset3_2)

        dataset4_1 = BigQueryDataset(
            dataset_id='test',
            friendly_name='test_friendly_name',
            description='test_description',
            default_table_expiration_ms=24 * 60 * 60 * 1000,
            location='US',
            access_entries=(access_entry1, )
        )
        dataset4_2 = BigQueryDataset(
            dataset_id='foo',
            friendly_name='bar',
            description='test_description',
            default_table_expiration_ms=24 * 60 * 60 * 1000,
            location='US',
            access_entries=(access_entry2, )
        )
        self.assertNotEqual(dataset4_1, dataset4_2)

        dataset5_1 = BigQueryDataset(
            dataset_id='foo',
            friendly_name='bar',
            description='test_description',
            default_table_expiration_ms=24 * 60 * 60 * 1000,
            location='US',
            access_entries=(access_entry1, access_entry2)
        )
        dataset5_2 = BigQueryDataset(
            dataset_id='foo',
            friendly_name='bar',
            description='test_description',
            default_table_expiration_ms=24 * 60 * 60 * 1000,
            location='US',
            access_entries=(access_entry2, access_entry1)
        )
        dataset5_3 = BigQueryDataset(
            dataset_id='foo',
            friendly_name='bar',
            description='test_description',
            default_table_expiration_ms=24 * 60 * 60 * 1000,
            location='US',
            access_entries=[access_entry1, access_entry2]
        )
        self.assertEqual(dataset5_1, dataset5_2)
        self.assertEqual(dataset5_1, dataset5_3)
        self.assertEqual(dataset5_2, dataset5_3)

        dataset6_1 = BigQueryDataset(
            dataset_id='foo',
            friendly_name='bar',
            description='test_description',
            default_table_expiration_ms=24 * 60 * 60 * 1000,
            location='US',
            access_entries=(access_entry1, access_entry2)
        )
        dataset6_2 = BigQueryDataset(
            dataset_id='foo',
            friendly_name='bar',
            description='test_description',
            default_table_expiration_ms=24 * 60 * 60 * 1000,
            location='US',
            access_entries=(access_entry3, )
        )
        dataset6_3 = BigQueryDataset(
            dataset_id='foo',
            friendly_name='bar',
            description='test_description',
            default_table_expiration_ms=24 * 60 * 60 * 1000,
            location='US',
            access_entries=(access_entry1, access_entry3)
        )
        self.assertNotEqual(dataset6_1, dataset6_2)
        self.assertNotEqual(dataset6_1, dataset6_3)
        self.assertNotEqual(dataset6_2, dataset6_3)

        label1 = {
            'foo': 'bar'
        }
        label2 = {
            'fizz': 'buzz'
        }

        dataset7_1 = BigQueryDataset(
            dataset_id='foo',
            friendly_name='bar',
            description='test_description',
            default_table_expiration_ms=24 * 60 * 60 * 1000,
            location='US',
            labels=label1
        )
        dataset7_2 = BigQueryDataset(
            dataset_id='foo',
            friendly_name='bar',
            description='test_description',
            default_table_expiration_ms=24 * 60 * 60 * 1000,
            location='US',
            labels=label1
        )
        self.assertEqual(dataset7_1, dataset7_2)

        dataset8_1 = BigQueryDataset(
            dataset_id='foo',
            friendly_name='bar',
            description='test_description',
            default_table_expiration_ms=24 * 60 * 60 * 1000,
            location='US',
            labels=label1
        )
        dataset8_2 = BigQueryDataset(
            dataset_id='foo',
            friendly_name='bar',
            description='test_description',
            default_table_expiration_ms=24 * 60 * 60 * 1000,
            location='US',
            labels=label2
        )
        self.assertNotEqual(dataset8_1, dataset8_2)

    def test_from_dict(self):
        expected_dataset1 = BigQueryDataset(
            dataset_id='test',
            friendly_name='test_friendly_name',
            description='test_description',
            default_table_expiration_ms=24 * 60 * 60 * 1000,
            location='US'
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
            dataset_id='test',
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
                },
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
                },
            ]
        })
        self.assertNotEqual(expected_dataset2, actual_dataset2_2)

        expected_dataset3 = BigQueryDataset(
            dataset_id='test',
            friendly_name='test_friendly_name',
            description='test_description',
            default_table_expiration_ms=24 * 60 * 60 * 1000,
            location='US',
            labels={
                'foo': 'bar'
            }
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

    def test_from_dataset(self):
        project = 'test'

        expected_dataset1 = BigQueryDataset(
            dataset_id='test',
            friendly_name='test_friendly_name',
            description='test_description',
            default_table_expiration_ms=24 * 60 * 60 * 1000,
            location='US'
        )
        actual_dataset1_1 = BigQueryDataset.from_dataset(make_dataset(
            project=project,
            dataset_id='test',
            friendly_name='test_friendly_name',
            description='test_description',
            default_table_expiration_ms=24 * 60 * 60 * 1000,
            location='US'
        ))
        self.assertEqual(expected_dataset1, actual_dataset1_1)
        actual_dataset1_2 = BigQueryDataset.from_dataset(make_dataset(
            project=project,
            dataset_id='test',
            friendly_name='foo_bar',
            description='fizz_buzz',
            default_table_expiration_ms=60 * 60 * 1000,
            location='EU'
        ))
        self.assertNotEqual(expected_dataset1, actual_dataset1_2)

        expected_dataset2 = BigQueryDataset(
            dataset_id='test',
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
        actual_dataset2_1 = BigQueryDataset.from_dataset(make_dataset(
            project=project,
            dataset_id='test',
            friendly_name='test_friendly_name',
            description='test_description',
            default_table_expiration_ms=24 * 60 * 60 * 1000,
            location='US',
            access_entries=(AccessEntry('OWNER', 'specialGroup', 'projectOwners'), )
        ))
        self.assertEqual(expected_dataset2, actual_dataset2_1)
        actual_dataset2_2 = BigQueryDataset.from_dataset(make_dataset(
            project=project,
            dataset_id='test',
            friendly_name='test_friendly_name',
            description='test_description',
            default_table_expiration_ms=24 * 60 * 60 * 1000,
            location='US',
            access_entries=(AccessEntry(None, 'view', {
                'datasetId': 'test',
                'projectId': 'test-project',
                'tableId': 'test_table'
            }), )
        ))
        self.assertNotEqual(expected_dataset2, actual_dataset2_2)

        expected_dataset3 = BigQueryDataset(
            dataset_id='test',
            friendly_name='test_friendly_name',
            description='test_description',
            default_table_expiration_ms=24 * 60 * 60 * 1000,
            location='US',
            labels={
                'foo': 'bar'
            }
        )
        actual_dataset3_1 = BigQueryDataset.from_dataset(make_dataset(
            project=project,
            dataset_id='test',
            friendly_name='test_friendly_name',
            description='test_description',
            default_table_expiration_ms=24 * 60 * 60 * 1000,
            location='US',
            labels={
                'foo': 'bar'
            }
        ))
        self.assertEqual(expected_dataset3, actual_dataset3_1)
        actual_dataset3_2 = BigQueryDataset.from_dataset(make_dataset(
            project=project,
            dataset_id='test',
            friendly_name='test_friendly_name',
            description='test_description',
            default_table_expiration_ms=24 * 60 * 60 * 1000,
            location='US',
            labels={
                'foo': 'bar',
                'fizz': 'buzz'
            }
        ))
        self.assertNotEqual(expected_dataset3, actual_dataset3_2)

    def test_to_dataset(self):
        project = 'test'

        expected_dataset1 = make_dataset(
            project=project,
            dataset_id='test',
            friendly_name='test_friendly_name',
            description='test_description',
            default_table_expiration_ms=24 * 60 * 60 * 1000,
            location='US'
        )
        actual_dataset1_1 = BigQueryDataset.to_dataset(project, BigQueryDataset(
            dataset_id='test',
            friendly_name='test_friendly_name',
            description='test_description',
            default_table_expiration_ms=24 * 60 * 60 * 1000,
            location='US'
        ))
        self.assertEqual(expected_dataset1.dataset_id, actual_dataset1_1.dataset_id)
        self.assertEqual(expected_dataset1.friendly_name, actual_dataset1_1.friendly_name)
        self.assertEqual(expected_dataset1.description, actual_dataset1_1.description)
        self.assertEqual(expected_dataset1.default_table_expiration_ms,
                         actual_dataset1_1.default_table_expiration_ms)
        self.assertEqual(expected_dataset1.location, actual_dataset1_1.location)
        self.assertEqual(expected_dataset1.labels, actual_dataset1_1.labels)
        self.assertEqual(expected_dataset1.access_entries, actual_dataset1_1.access_entries)
        actual_dataset1_2 = BigQueryDataset.to_dataset(project, BigQueryDataset(
            dataset_id='aaa',
            friendly_name='foo_bar',
            description='fizz_buzz',
            default_table_expiration_ms=60 * 60 * 1000,
            location='EU'
        ))
        self.assertNotEqual(expected_dataset1.dataset_id, actual_dataset1_2.dataset_id)
        self.assertNotEqual(expected_dataset1.friendly_name, actual_dataset1_2.friendly_name)
        self.assertNotEqual(expected_dataset1.description, actual_dataset1_2.description)
        self.assertNotEqual(expected_dataset1.default_table_expiration_ms,
                            actual_dataset1_2.default_table_expiration_ms)
        self.assertNotEqual(expected_dataset1.location, actual_dataset1_2.location)
        self.assertEqual(expected_dataset1.labels, actual_dataset1_2.labels)
        self.assertEqual(expected_dataset1.access_entries, actual_dataset1_2.access_entries)

        expected_dataset2 = make_dataset(
            project=project,
            dataset_id='test',
            friendly_name='test_friendly_name',
            description='test_description',
            default_table_expiration_ms=24 * 60 * 60 * 1000,
            location='US',
            access_entries=(
                AccessEntry(
                    'OWNER',
                    'specialGroup',
                    'projectOwners'
                ),
            )
        )
        actual_dataset2_1 = BigQueryDataset.to_dataset(project, BigQueryDataset(
            dataset_id='test',
            friendly_name='test_friendly_name',
            description='test_description',
            default_table_expiration_ms=24 * 60 * 60 * 1000,
            location='US',
            access_entries=(
                BigQueryAccessEntry('OWNER', 'specialGroup', 'projectOwners'),
            )
        ))
        self.assertEqual(expected_dataset2.access_entries, actual_dataset2_1.access_entries)
        actual_dataset2_2 = BigQueryDataset.to_dataset(project, BigQueryDataset(
            dataset_id='test',
            friendly_name='test_friendly_name',
            description='test_description',
            default_table_expiration_ms=24 * 60 * 60 * 1000,
            location='US',
            access_entries=(
                BigQueryAccessEntry(None, 'view', {
                    'datasetId': 'test',
                    'projectId': 'test-project',
                    'tableId': 'test_table'
                }),
            )
        ))
        self.assertNotEqual(expected_dataset2.access_entries, actual_dataset2_2.access_entries)

        expected_dataset3 = make_dataset(
            project=project,
            dataset_id='test',
            friendly_name='test_friendly_name',
            description='test_description',
            default_table_expiration_ms=24 * 60 * 60 * 1000,
            location='US',
            labels={
                'foo': 'bar'
            }
        )
        actual_dataset3_1 = BigQueryDataset.to_dataset(project, BigQueryDataset(
            dataset_id='test',
            friendly_name='test_friendly_name',
            description='test_description',
            default_table_expiration_ms=24 * 60 * 60 * 1000,
            location='US',
            labels={
                'foo': 'bar'
            }
        ))
        self.assertEqual(expected_dataset3.labels, actual_dataset3_1.labels)
        actual_dataset3_2 = BigQueryDataset.to_dataset(project, BigQueryDataset(
            dataset_id='test',
            friendly_name='test_friendly_name',
            description='test_description',
            default_table_expiration_ms=24 * 60 * 60 * 1000,
            location='US',
            labels={
                'foo': 'bar',
                'fizz': 'buzz'
            }
        ))
        self.assertNotEqual(expected_dataset3.labels, actual_dataset3_2.labels)
