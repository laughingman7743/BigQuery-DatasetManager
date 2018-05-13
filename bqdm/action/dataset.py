# -*- coding: utf-8 -*-
from __future__ import absolute_import

import codecs
import logging
import os
import sys

from future.utils import iteritems
from google.cloud import bigquery
from google.oauth2 import service_account

from bqdm.model.dataset import BigQueryDataset
from bqdm.util import dump, echo, echo_dump, echo_ndiff

_logger = logging.getLogger(__name__)
_logger.addHandler(logging.StreamHandler(sys.stdout))
_logger.setLevel(logging.INFO)


class DatasetAction(object):

    def __init__(self, project=None, credential_file=None, no_color=False, debug=False):
        credentials = None
        if credential_file:
            credentials = service_account.Credentials.from_service_account_file(credential_file)
        self._client = bigquery.Client(project, credentials)
        self.no_color = no_color
        if debug:
            _logger.setLevel(logging.DEBUG)

    @staticmethod
    def get_add_datasets(source, target):
        dataset_ids = set(t.dataset_id for t in target) - set(s.dataset_id for s in source)
        results = [t for t in target if t.dataset_id in dataset_ids]
        return len(results), tuple(results)

    @staticmethod
    def get_change_datasets(source, target):
        _, add_datasets = DatasetAction.get_add_datasets(source, target)
        results = (set(target) - set(add_datasets)) - set(source)
        return len(results), tuple(results)

    @staticmethod
    def get_destroy_datasets(source, target):
        dataset_ids = set(s.dataset_id for s in source) - set(t.dataset_id for t in target)
        results = [s for s in source if s.dataset_id in dataset_ids]
        return len(results), tuple(results)

    @staticmethod
    def get_intersection_datasets(source, target):
        dataset_ids = set(s.dataset_id for s in source) & set(t.dataset_id for t in target)
        results = [s for s in source if s.dataset_id in dataset_ids]
        return len(results), tuple(results)

    def get_dataset(self, dataset_id):
        dataset_ref = self._client.dataset(dataset_id)
        dataset = self._client.get_dataset(dataset_ref)
        echo('Load dataset: ' + dataset.path)
        return dataset

    def list_datasets(self, dataset_id=None):
        datasets = []
        if dataset_id:
            datasets.append(BigQueryDataset.from_dataset(self.get_dataset(dataset_id)))
        else:
            for dataset in self._client.list_datasets():
                # TODO ThreadPoolExecutor
                datasets.append(BigQueryDataset.from_dataset(
                    self.get_dataset(dataset.dataset_id)))
        if datasets:
            echo('------------------------------------------------------------------------')
            echo()
        return datasets

    def export(self, output_dir, dataset_id=None):
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        datasets = self.list_datasets(dataset_id)
        for dataset in datasets:
            data = dump(BigQueryDataset.from_dataset(dataset))
            _logger.debug(data)
            export_path = os.path.join(output_dir, '{0}.yml'.format(dataset.dataset_id))
            echo('Export dataset config: {0}'.format(export_path))
            with codecs.open(export_path, 'wb', 'utf-8') as f:
                f.write(data)
        echo()
        return datasets

    def _add(self, model):
        dataset = BigQueryDataset.to_dataset(self._client.project, model)
        echo('  Adding... {0}'.format(dataset.path), fg='green', no_color=self.no_color)
        echo_dump(model)
        self._client.create_dataset(dataset)
        self._client.update_dataset(dataset, [
            'access_entries'
        ])
        echo()

    def plan_add(self, source, target):
        count, datasets = self.get_add_datasets(source, target)
        _logger.debug('Add datasets: {0}'.format(datasets))
        for dataset in datasets:
            echo('  + {0}'.format(dataset.dataset_id), fg='green', no_color=self.no_color)
            echo_dump(dataset)
            echo()
        return count

    def add(self, source, target):
        count, datasets = self.get_add_datasets(source, target)
        _logger.debug('Add datasets: {0}'.format(datasets))
        for dataset in datasets:
            self._add(dataset)
        return count

    def _change(self, source_model, target_model):
        dataset = BigQueryDataset.to_dataset(self._client.project, target_model)
        echo('  Changing... {0}'.format(dataset.path), fg='yellow', no_color=self.no_color)
        echo_ndiff(source_model, target_model)
        source_labels = source_model.labels
        if source_labels:
            labels = dataset.labels.copy()
            for k, v in iteritems(source_labels):
                if k not in labels.keys():
                    labels[k] = None
            dataset.labels = labels
        self._client.update_dataset(dataset, [
            'friendly_name',
            'description',
            'default_table_expiration_ms',
            'labels',
            'access_entries'
        ])
        echo()

    def plan_change(self, source, target):
        count, datasets = self.get_change_datasets(source, target)
        _logger.debug('Change datasets: {0}'.format(datasets))
        for dataset in datasets:
            echo('  ~ {0}'.format(dataset.dataset_id), fg='yellow', no_color=self.no_color)
            source_dataset = next((s for s in source if s.dataset_id == dataset.dataset_id), None)
            echo_ndiff(source_dataset, dataset)
            echo()
        return count

    def change(self, source, target):
        count, datasets = self.get_change_datasets(source, target)
        _logger.debug('Change datasets: {0}'.format(datasets))
        for dataset in datasets:
            source_dataset = next((s for s in source if s.dataset_id == dataset.dataset_id), None)
            self._change(source_dataset, dataset)
        return count

    def _destroy(self, model):
        datasetted = BigQueryDataset.to_dataset(self._client.project, model)
        echo('  Destroying... {0}'.format(datasetted.path), fg='red', no_color=self.no_color)
        self._client.delete_dataset(datasetted)
        echo()

    def plan_destroy(self, source, target):
        count, datasets = self.get_destroy_datasets(source, target)
        _logger.debug('Destroy datasets: {0}'.format(datasets))
        for dataset in datasets:
            echo('  - {0}'.format(dataset.dataset_id), fg='red', no_color=self.no_color)
            echo()
        return count

    def destroy(self, source, target):
        count, datasets = self.get_destroy_datasets(source, target)
        _logger.debug('Destroy datasets: {0}'.format(datasets))
        for dataset in datasets:
            self._destroy(dataset)
        return count

    def plan_intersection_destroy(self, source, target):
        count, datasets = self.get_intersection_datasets(target, source)
        _logger.debug('Destroy datasets: {0}'.format(datasets))
        for dataset in datasets:
            echo('  - {0}'.format(dataset.dataset_id), fg='red', no_color=self.no_color)
            echo()
        return count

    def intersection_destroy(self, source, target):
        count, datasets = self.get_intersection_datasets(target, source)
        _logger.debug('Destroy datasets: {0}'.format(datasets))
        for dataset in datasets:
            self._destroy(dataset)
        return count
