# -*- coding: utf-8 -*-
import unittest

from bqdm.model import BigQueryDataset, BigQueryAccessGrant


class TestModel(unittest.TestCase):

    def test_eq_dataset(self):
        dataset1 = BigQueryDataset(
            'test',
            'test_friendly_name',
            'test_description',
            24 * 30 * 60 * 1000,
            'US',
            None
        )
        dataset2 = BigQueryDataset(
            'test',
            'test_friendly_name',
            'test_description',
            24 * 30 * 60 * 1000,
            'US',
            None
        )
        dataset3 = BigQueryDataset(
            'test',
            'test_friendly_name',
            'test_description',
            24 * 30 * 60 * 1000,
            'EU',
            None
        )
        dataset4 = BigQueryDataset(
            'test',
            'test_friendly_name',
            'test_description',
            None,
            'EU',
            None
        )
        self.assertEqual(dataset1, dataset2)
        self.assertNotEqual(dataset1, dataset3)
        self.assertNotEqual(dataset1, dataset4)
        self.assertNotEqual(dataset3, dataset4)

    def test_eq_dataset_with_access_grant(self):
        access_grant1 = BigQueryAccessGrant(
            'OWNER',
            'specialGroup',
            'projectOwners'
        )
        access_grant2 = BigQueryAccessGrant(
            'OWNER',
            'specialGroup',
            'projectOwners'
        )
        access_grant3 = BigQueryAccessGrant(
            None,
            'view',
            {
                'datasetId': 'test',
                'projectId': 'test-project',
                'tableId': 'test_table'
            }
        )

        dataset_with_access_grant1 = BigQueryDataset(
            'test',
            'test_friendly_name',
            'test_description',
            24 * 30 * 60 * 1000,
            'US',
            [access_grant1]
        )
        dataset_with_access_grant2 = BigQueryDataset(
            'test',
            'test_friendly_name',
            'test_description',
            24 * 30 * 60 * 1000,
            'US',
            [access_grant2]
        )
        dataset_with_access_grant3 = BigQueryDataset(
            'test',
            'test_friendly_name',
            'test_description',
            24 * 30 * 60 * 1000,
            'US',
            [access_grant3]
        )
        dataset_with_access_grant4 = BigQueryDataset(
            'foo',
            'bar',
            'test_description',
            24 * 30 * 60 * 1000,
            'US',
            [access_grant1]
        )
        dataset_with_access_grant5 = BigQueryDataset(
            'foo',
            'bar',
            'test_description',
            24 * 30 * 60 * 1000,
            'US',
            [access_grant1, access_grant3]
        )
        dataset_with_access_grant6 = BigQueryDataset(
            'foo',
            'bar',
            'test_description',
            24 * 30 * 60 * 1000,
            'US',
            [access_grant3, access_grant1]
        )
        dataset_with_access_grant7 = BigQueryDataset(
            'foo',
            'bar',
            'test_description',
            24 * 30 * 60 * 1000,
            'US',
            [access_grant1, access_grant2]
        )
        self.assertEqual(dataset_with_access_grant1, dataset_with_access_grant2)
        self.assertNotEqual(dataset_with_access_grant1, dataset_with_access_grant3)
        self.assertNotEqual(dataset_with_access_grant1, dataset_with_access_grant4)
        self.assertNotEqual(dataset_with_access_grant3, dataset_with_access_grant4)
        self.assertEqual(dataset_with_access_grant5, dataset_with_access_grant6)
        self.assertEqual(dataset_with_access_grant4, dataset_with_access_grant7)
        self.assertNotEqual(dataset_with_access_grant6, dataset_with_access_grant7)

    def test_dataset_from_dict(self):
        dataset = BigQueryDataset(
            'test',
            'test_friendly_name',
            'test_description',
            24 * 30 * 60 * 1000,
            'US',
            None
        )
        dataset_from_dict1 = BigQueryDataset.from_dict({
            'name': 'test',
            'friendly_name': 'test_friendly_name',
            'description': 'test_description',
            'default_table_expiration_ms': 24 * 30 * 60 * 1000,
            'location': 'US',
            'access_grants': None
        })
        dataset_from_dict2 = BigQueryDataset.from_dict({
            'name': 'test',
            'friendly_name': 'test_friendly_name',
            'description': 'test_description',
            'default_table_expiration_ms': None,
            'location': 'US',
            'access_grants': None
        })
        dataset_from_dict3 = BigQueryDataset.from_dict({
            'name': 'foo',
            'friendly_name': 'bar',
            'description': 'test_description',
            'default_table_expiration_ms': None,
            'location': 'US',
            'access_grants': None
        })
        self.assertEqual(dataset, dataset_from_dict1)
        self.assertNotEqual(dataset, dataset_from_dict2)
        self.assertNotEqual(dataset, dataset_from_dict3)

    def test_dataset_from_dict_with_access_grant(self):
        access_grant1 = BigQueryAccessGrant(
            'OWNER',
            'specialGroup',
            'projectOwners'
        )
        access_grant2 = BigQueryAccessGrant(
            None,
            'view',
            {
                'datasetId': 'test',
                'projectId': 'test-project',
                'tableId': 'test_table'
            }
        )

        dataset_with_access_grant1 = BigQueryDataset(
            'test',
            'test_friendly_name',
            'test_description',
            24 * 30 * 60 * 1000,
            'US',
            [access_grant1]
        )
        dataset_with_access_grant2 = BigQueryDataset(
            'test',
            'test_friendly_name',
            'test_description',
            24 * 30 * 60 * 1000,
            'US',
            [access_grant2]
        )
        dataset_with_access_grant3 = BigQueryDataset(
            'test',
            'test_friendly_name',
            'test_description',
            24 * 30 * 60 * 1000,
            'US',
            [access_grant1, access_grant2]
        )
        dataset_with_access_grant4 = BigQueryDataset(
            'test',
            'test_friendly_name',
            'test_description',
            24 * 30 * 60 * 1000,
            'US',
            [access_grant1, access_grant1]
        )
        dataset_with_access_grant_from_dict1 = BigQueryDataset.from_dict({
            'name': 'test',
            'friendly_name': 'test_friendly_name',
            'description': 'test_description',
            'default_table_expiration_ms': 24 * 30 * 60 * 1000,
            'location': 'US',
            'access_grants': [
                {
                    'role': 'OWNER',
                    'entity_type': 'specialGroup',
                    'entity_id': 'projectOwners'
                }
            ]
        })
        dataset_with_access_grant_from_dict2 = BigQueryDataset.from_dict({
            'name': 'test',
            'friendly_name': 'test_friendly_name',
            'description': 'test_description',
            'default_table_expiration_ms': 24 * 30 * 60 * 1000,
            'location': 'US',
            'access_grants': [
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
        dataset_with_access_grant_from_dict3 = BigQueryDataset.from_dict({
            'name': 'test',
            'friendly_name': 'test_friendly_name',
            'description': 'test_description',
            'default_table_expiration_ms': 24 * 30 * 60 * 1000,
            'location': 'US',
            'access_grants': [
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
        dataset_with_access_grant_from_dict4 = BigQueryDataset.from_dict({
            'name': 'test',
            'friendly_name': 'test_friendly_name',
            'description': 'test_description',
            'default_table_expiration_ms': 24 * 30 * 60 * 1000,
            'location': 'US',
            'access_grants': [
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
        self.assertEqual(dataset_with_access_grant1, dataset_with_access_grant_from_dict1)
        self.assertNotEqual(dataset_with_access_grant1, dataset_with_access_grant_from_dict2)
        self.assertNotEqual(dataset_with_access_grant1, dataset_with_access_grant_from_dict3)
        self.assertEqual(dataset_with_access_grant2, dataset_with_access_grant_from_dict2)
        self.assertNotEqual(dataset_with_access_grant2, dataset_with_access_grant_from_dict1)
        self.assertNotEqual(dataset_with_access_grant2, dataset_with_access_grant_from_dict3)
        self.assertEqual(dataset_with_access_grant3, dataset_with_access_grant_from_dict3)
        self.assertNotEqual(dataset_with_access_grant3, dataset_with_access_grant_from_dict1)
        self.assertNotEqual(dataset_with_access_grant3, dataset_with_access_grant_from_dict2)
        self.assertEqual(dataset_with_access_grant4, dataset_with_access_grant_from_dict4)
        self.assertEqual(dataset_with_access_grant1, dataset_with_access_grant_from_dict4)
        self.assertEqual(dataset_with_access_grant4, dataset_with_access_grant_from_dict1)
