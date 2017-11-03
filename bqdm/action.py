# -*- coding: utf-8 -*-
import codecs
import logging
import os
import sys

import click
from google.cloud import bigquery

from bqdm.model import BigQueryDataset
from bqdm.util import dump_dataset, ndiff


_logger = logging.getLogger(__name__)
_logger.addHandler(logging.StreamHandler(sys.stdout))
_logger.setLevel(logging.INFO)


class DatasetAction(object):

    def __init__(self, debug=False):
        self.client = bigquery.Client()
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

    def list_datasets(self):
        datasets = []
        for dataset in self.client.list_datasets():
            click.echo('Load: ' + dataset.path)
            datasets.append(BigQueryDataset.from_dataset(self.client, dataset.dataset_id))
        click.echo('------------------------------------------------------------------------')
        click.echo()
        return datasets

    def export(self, output_dir):
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        for dataset in self.client.list_datasets():
            click.echo('Export: {0}'.format(dataset.path))
            data = dump_dataset(BigQueryDataset.from_dataset(self.client, dataset.dataset_id))
            _logger.debug(data)
            with codecs.open(os.path.join(output_dir, '{0}.yml'.format(dataset.dataset_id)),
                             'wb', 'utf-8') as f:
                f.write(data)

    def plan_add(self, source, target):
        count, datasets = self.get_add_datasets(source, target)
        _logger.debug('Add datasets: {0}'.format(datasets))
        for dataset in datasets:
            click.secho('  + {0}'.format(dataset.dataset_id), fg='green')
            for line in dump_dataset(dataset).splitlines():
                click.echo('    {0}'.format(line))
            click.echo()
        return count

    def add(self, source, target):
        count, datasets = self.get_add_datasets(source, target)
        _logger.debug('Add datasets: {0}'.format(datasets))
        for dataset in datasets:
            converted = BigQueryDataset.to_dataset(self.client, dataset)
            click.secho('  Adding... {0}'.format(converted.path), fg='green')
            for line in dump_dataset(dataset).splitlines():
                click.echo('    {0}'.format(line))
            self.client.create_dataset(converted)
            self.client.update_dataset(converted, [
                'access_entries'
            ])
            click.echo()
        return count

    def plan_change(self, source, target):
        count, datasets = self.get_change_datasets(source, target)
        _logger.debug('Change datasets: {0}'.format(datasets))
        for dataset in datasets:
            click.secho('  ~ {0}'.format(dataset.dataset_id), fg='yellow')
            old_dataset = next((o for o in source if o.dataset_id == dataset.dataset_id), None)
            for d in ndiff(old_dataset, dataset):
                click.echo('    {0}'.format(d))
            click.echo()
        return count

    def change(self, source, target):
        count, datasets = self.get_change_datasets(source, target)
        _logger.debug('Change datasets: {0}'.format(datasets))
        for dataset in datasets:
            converted = BigQueryDataset.to_dataset(self.client, dataset)
            old_dataset = next((o for o in source if o.dataset_id == dataset.dataset_id), None)
            diff = ndiff(old_dataset, dataset)
            click.secho('  Changing... {0}'.format(converted.path), fg='yellow')
            for d in diff:
                click.echo('    {0}'.format(d))
            self.client.update_dataset(converted, [
                'friendly_name',
                'description',
                'default_table_expiration_ms',
                'access_entries'
            ])
            click.echo()
        return count

    def plan_destroy(self, source, target):
        count, datasets = self.get_destroy_datasets(source, target)
        _logger.debug('Destroy datasets: {0}'.format(datasets))
        for dataset in datasets:
            click.secho('  - {0}'.format(dataset.dataset_id), fg='red')
            click.echo()
        return count

    def destroy(self, source, target):
        count, datasets = self.get_destroy_datasets(source, target)
        _logger.debug('Destroy datasets: {0}'.format(datasets))
        for dataset in datasets:
            converted = BigQueryDataset.to_dataset(self.client, dataset)
            click.secho('  Destroying... {0}'.format(converted.path), fg='red')
            self.client.delete_dataset(converted)
            click.echo()
        return count

    def plan_intersection_destroy(self, source, target):
        count, datasets = self.get_intersection_datasets(target, source)
        _logger.debug('Destroy datasets: {0}'.format(datasets))
        for dataset in datasets:
            click.secho('  - {0}'.format(dataset.dataset_id), fg='red')
            click.echo()
        return count

    def intersection_destroy(self, source, target):
        count, datasets = self.get_intersection_datasets(target, source)
        _logger.debug('Destroy datasets: {0}'.format(datasets))
        for dataset in datasets:
            converted = BigQueryDataset.to_dataset(self.client, dataset)
            click.secho('  Destroying... {0}'.format(dataset.dataset_id), fg='red')
            self.client.delete_dataset(converted)
            click.echo()
        return count
