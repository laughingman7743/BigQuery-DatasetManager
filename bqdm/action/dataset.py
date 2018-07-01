# -*- coding: utf-8 -*-
from __future__ import absolute_import

import codecs
import logging
import os
import sys

from future.utils import iteritems
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from google.oauth2 import service_account

from bqdm.model.dataset import BigQueryDataset
from bqdm.util import (dump, echo, echo_dump, echo_ndiff)

_logger = logging.getLogger(__name__)
_logger.addHandler(logging.StreamHandler(sys.stdout))
_logger.setLevel(logging.INFO)


class DatasetAction(object):

    def __init__(self, executor, project=None, credential_file=None,
                 no_color=False, debug=False):
        self._executor = executor
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
        dataset = None
        try:
            dataset = self._client.get_dataset(dataset_ref)
            echo('Load dataset: ' + dataset.path)
            dataset = BigQueryDataset.from_dataset(dataset)
        except NotFound:
            _logger.info('Dataset {0} is not found.'.format(dataset_id))
        return dataset

    def _list_datasets(self, include_datasets=(), exclude_datasets=()):
        if not include_datasets:
            include_datasets = [d.dataset_id for d in self._client.list_datasets()]
        return tuple(set(include_datasets) - set(exclude_datasets))

    def list_datasets(self, include_datasets=(), exclude_datasets=()):
        fs = [self._executor.submit(self.get_dataset, d)
              for d in self._list_datasets(include_datasets, exclude_datasets)]
        return fs

    def _export(self, output_dir, dataset_id):
        dataset = self.get_dataset(dataset_id)
        if dataset:
            data = dump(dataset)
            _logger.debug(data)
            export_path = os.path.join(output_dir, '{0}.yml'.format(dataset.dataset_id))
            echo('Export dataset config: {0}'.format(export_path))
            with codecs.open(export_path, 'wb', 'utf-8') as f:
                f.write(data)
        return dataset

    def export(self, output_dir, include_datasets=(), exclude_datasets=()):
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        datasets = self._list_datasets(include_datasets, exclude_datasets)
        fs = [self._executor.submit(self._export, output_dir, d)
              for d in datasets]
        return fs

    def _add(self, model, prefix='  ', fg='green'):
        dataset = BigQueryDataset.to_dataset(self._client.project, model)
        echo('Adding... {0}'.format(dataset.path),
             prefix=prefix, fg=fg, no_color=self.no_color)
        echo_dump(model, prefix=prefix + '  ', fg=fg, no_color=self.no_color)
        self._client.create_dataset(dataset)
        self._client.update_dataset(dataset, [
            'access_entries'
        ])
        echo()

    def plan_add(self, source, target, prefix='  ', fg='green'):
        count, datasets = self.get_add_datasets(source, target)
        _logger.debug('Add datasets: {0}'.format(datasets))
        for dataset in datasets:
            echo('+ {0}'.format(dataset.dataset_id),
                 prefix=prefix, fg=fg, no_color=self.no_color)
            echo_dump(dataset, prefix=prefix + '  ', fg=fg, no_color=self.no_color)
            echo()
        return count

    def add(self, source, target, prefix='  ', fg='green'):
        count, datasets = self.get_add_datasets(source, target)
        _logger.debug('Add datasets: {0}'.format(datasets))
        fs = [self._executor.submit(self._add, d, prefix, fg) for d in datasets]
        return count, fs

    def _change(self, source_model, target_model, prefix='  ', fg='yellow'):
        dataset = BigQueryDataset.to_dataset(self._client.project, target_model)
        echo('Changing... {0}'.format(dataset.path),
             prefix=prefix, fg=fg, no_color=self.no_color)
        echo_ndiff(source_model, target_model, prefix=prefix + '  ', fg=fg)
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

    def plan_change(self, source, target, prefix='  ', fg='yellow'):
        count, datasets = self.get_change_datasets(source, target)
        _logger.debug('Change datasets: {0}'.format(datasets))
        for dataset in datasets:
            echo('~ {0}'.format(dataset.dataset_id),
                 prefix=prefix, fg=fg, no_color=self.no_color)
            source_dataset = next((s for s in source if s.dataset_id == dataset.dataset_id), None)
            echo_ndiff(source_dataset, dataset, prefix=prefix + '  ', fg=fg)
            echo()
        return count

    def change(self, source, target, prefix='  ', fg='yellow'):
        count, datasets = self.get_change_datasets(source, target)
        _logger.debug('Change datasets: {0}'.format(datasets))
        fs = [self._executor.submit(
            self._change, next((s for s in source if s.dataset_id == d.dataset_id), None),
            d, prefix, fg) for d in datasets]
        return count, fs

    def _destroy(self, model, prefix='  ', fg='red'):
        datasetted = BigQueryDataset.to_dataset(self._client.project, model)
        echo('Destroying... {0}'.format(datasetted.path),
             prefix=prefix, fg=fg, no_color=self.no_color)
        self._client.delete_dataset(datasetted)
        echo()

    def plan_destroy(self, source, target, prefix='  ', fg='red'):
        count, datasets = self.get_destroy_datasets(source, target)
        _logger.debug('Destroy datasets: {0}'.format(datasets))
        for dataset in datasets:
            echo('  - {0}'.format(dataset.dataset_id),
                 prefix=prefix, fg=fg, no_color=self.no_color)
            echo()
        return count

    def destroy(self, source, target):
        count, datasets = self.get_destroy_datasets(source, target)
        _logger.debug('Destroy datasets: {0}'.format(datasets))
        fs = [self._executor.submit(self._destroy, d) for d in datasets]
        return count, fs

    def plan_intersection_destroy(self, source, target, prefix='  ', fg='red'):
        count, datasets = self.get_intersection_datasets(target, source)
        _logger.debug('Destroy datasets: {0}'.format(datasets))
        for dataset in datasets:
            echo('- {0}'.format(dataset.dataset_id),
                 prefix=prefix, fg=fg, no_color=self.no_color)
            echo()
        return count

    def intersection_destroy(self, source, target, prefix='  ', fg='red'):
        count, datasets = self.get_intersection_datasets(target, source)
        _logger.debug('Destroy datasets: {0}'.format(datasets))
        fs = [self._executor.submit(self._destroy, d, prefix, fg) for d in datasets]
        return count, fs
