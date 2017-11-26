# -*- coding: utf-8 -*-
from __future__ import absolute_import
import codecs
import copy
import logging
import os
import sys
import uuid

import click
from future.utils import iteritems
from google.cloud import bigquery
from google.cloud.bigquery.job import WriteDisposition, QueryJobConfig

from bqdm.model import BigQueryDataset, BigQueryTable, BigQuerySchemaField
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
            dataset_ref = self.client.dataset(dataset.dataset_id)
            dataset_detail = self.client.get_dataset(dataset_ref)
            click.echo('Load: ' + dataset_detail.path)
            datasets.append(BigQueryDataset.from_dataset(dataset_detail))
        click.echo('------------------------------------------------------------------------')
        click.echo()
        return datasets

    def export(self, output_dir):
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        datasets = self.list_datasets()
        for dataset in datasets:
            data = dump_dataset(BigQueryDataset.from_dataset(dataset))
            _logger.debug(data)
            export_path = os.path.join(output_dir, '{0}.yml'.format(dataset.dataset_id))
            click.echo('Export: {0}'.format(export_path))
            with codecs.open(export_path, 'wb', 'utf-8') as f:
                f.write(data)
        click.echo()
        return datasets

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
            old_dataset = next((s for s in source if s.dataset_id == dataset.dataset_id), None)
            for d in ndiff(old_dataset, dataset):
                click.secho('    {0}'.format(d), fg='yellow')
            click.echo()
        return count

    def change(self, source, target):
        count, datasets = self.get_change_datasets(source, target)
        _logger.debug('Change datasets: {0}'.format(datasets))
        for dataset in datasets:
            converted = BigQueryDataset.to_dataset(self.client, dataset)
            old_dataset = next((s for s in source if s.dataset_id == dataset.dataset_id), None)
            diff = ndiff(old_dataset, dataset)
            click.secho('  Changing... {0}'.format(converted.path), fg='yellow')
            for d in diff:
                click.secho('    {0}'.format(d), fg='yellow')
            old_labels = old_dataset.labels
            if old_labels:
                labels = converted.labels.copy()
                for k, v in iteritems(old_labels):
                    if k not in labels.keys():
                        labels[k] = None
                converted.labels = labels
            self.client.update_dataset(converted, [
                'friendly_name',
                'description',
                'default_table_expiration_ms',
                'labels',
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


class SchemaMigrationMode(object):

    SELECT_INSERT = 'select_insert'
    SELECT_INSERT_EMPTY = 'select_insert_empty'
    DROP_CREATE = 'drop_create'
    DROP_CREATE_EMPTY = 'drop_create_empty'


class TableAction(object):

    def __init__(self, dataset_id,
                 migration_mode=SchemaMigrationMode.SELECT_INSERT, debug=False):
        self.client = bigquery.Client()
        dataset_ref = self.client.dataset(dataset_id)
        self.dataset = self.client.get_dataset(dataset_ref)
        self.migration_mode = migration_mode
        if debug:
            _logger.setLevel(logging.DEBUG)

    @staticmethod
    def get_add_tables(source, target):
        table_ids = set(t.table_id for t in target) - set(s.table_id for s in source)
        results = [t for t in target if t.table_id in table_ids]
        return len(results), tuple(results)

    @staticmethod
    def get_change_tables(source, target):
        _, add_tables = TableAction.get_add_tables(source, target)
        results = (set(target) - set(add_tables)) - set(source)
        return len(results), tuple(results)

    @staticmethod
    def get_destroy_tables(source, target):
        table_ids = set(s.table_id for s in source) - set(t.table_id for t in target)
        results = [s for s in source if s.table_id in table_ids]
        return len(results), tuple(results)

    def migrate(self, source_table, target_table):
        if self.migration_mode == SchemaMigrationMode.SELECT_INSERT:
            query_field = self.build_query_field(source_table.schema, target_table.schema)
            self.select_insert(target_table.table_id, target_table.table_id, query_field)
        elif self.migration_mode == SchemaMigrationMode.SELECT_INSERT_EMPTY:
            query_field = self.build_query_field((), target_table.schema)
            self.select_insert(target_table.table_id, target_table.table_id, query_field)
        elif self.migration_mode == SchemaMigrationMode.DROP_CREATE:
            converted = BigQueryTable.to_table(self.dataset, target_table)
            tmp_table = self.create_temporary_table(target_table)
            query_field = self.build_query_field(source_table.schema, target_table.schema)
            self.select_insert(source_table.table_id, tmp_table.table_id, query_field)
            click.secho('    Dropping... {0}'.format(converted.path), fg='yellow')
            self.client.delete_table(converted)
            click.secho('    Creating... {0}'.format(converted.path), fg='yellow')
            self.client.create_table(converted)
            self.select_insert(tmp_table.table_id, target_table.table_id, '*')
            self.client.delete_table(tmp_table)
        else:
            converted = BigQueryTable.to_table(self.dataset, target_table)
            click.secho('    Dropping... {0}'.format(converted.path), fg='yellow')
            self.client.delete_table(converted)
            click.secho('    Creating... {0}'.format(converted.path), fg='yellow')
            self.client.create_table(converted)

    def select_insert(self, source_table_id, destination_table_id, query_field):
        query = 'SELECT {query_field} FROM {dataset_id}.{source_table_id}'.format(
            query_field=query_field,
            dataset_id=self.dataset.dataset_id,
            source_table_id=source_table_id)

        destination_table = self.dataset.table(destination_table_id)
        job_config = QueryJobConfig()
        job_config.use_legacy_sql = False
        job_config.use_query_cache = False
        job_config.write_disposition = WriteDisposition.WRITE_TRUNCATE
        job_config.destination = destination_table
        query_job = self.client.query(query, job_config)
        click.secho('    Inserting... {0}'.format(query_job.job_id), fg='yellow')
        click.secho('      {0}'.format(query_job.query), fg='yellow')
        query_job.result()
        assert query_job.state == 'DONE'
        error_result = query_job.error_result
        if error_result:
            raise RuntimeError(query_job.errors)

    def build_query_field(self, source_schema, target_schema, prefix=None):
        query_fields = []
        for target in target_schema:
            source = next((s for s in source_schema if s.name == target.name), None)
            if source:
                if target.field_type == 'RECORD':
                    field_prefix = '{0}.{1}'.format(prefix, target.name) if prefix else target.name
                    fields = self.build_query_field(source.fields, target.fields, field_prefix)
                    query_fields.append('struct({fields}) as {alias}'.format(
                        fields=fields, alias=target.name))
                else:
                    name = '{0}.{1}'.format(prefix, target.name) if prefix else target.name
                    field_type = BigQuerySchemaField.normalize_field_type(target.field_type)
                    query_fields.append('cast({name} AS {type}) AS {alias}'.format(
                        name=name, type=field_type, alias=target.name))
            else:
                if target.field_type == 'RECORD':
                    field_prefix = '{0}.{1}'.format(prefix, target.name) if prefix else target.name
                    fields = self.build_query_field((), target.fields, field_prefix)
                    query_fields.append('struct({fields}) as {alias}'.format(
                        fields=fields, alias=target.name))
                else:
                    query_fields.append('null AS {alias}'.format(alias=target.name))
        return ', '.join(query_fields)

    def create_temporary_table(self, model):
        tmp_table_model = copy.deepcopy(model)
        tmp_table_id = str(uuid.uuid4()).replace('-', '_')
        tmp_table_model.table_id = tmp_table_id
        converted = BigQueryTable.to_table(self.dataset, model)
        click.secho('    Creating... {0}'.format(converted.path), fg='yellow')
        self.client.create_table(converted)
        return converted

    def list_tables(self):
        tables = []
        for table in self.client.list_dataset_tables(self.dataset):
            table_ref = self.dataset.table(table.table_id)
            table_detail = self.client.get_table(table_ref)
            click.echo('Load: ' + table_detail.path)
            tables.append(BigQueryTable.from_table(table_detail))
        click.echo('------------------------------------------------------------------------')
        click.echo()
        return tables

    def export(self, output_dir):
        output_dir = os.path.join(output_dir, self.dataset.dataset_id)
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        tables = self.list_tables()
        if not tables:
            keep_file = os.path.join(output_dir, '.gitkeep')
            if not os.path.exists(keep_file):
                open(keep_file, 'a').close()
        for table in tables:
            data = dump_dataset(BigQueryTable.from_table(table))
            _logger.debug(data)
            export_path = os.path.join(output_dir, '{0}.yml'.format(table.table_id))
            click.echo('Export: {0}'.format(export_path))
            with codecs.open(export_path, 'wb', 'utf-8') as f:
                f.write(data)
        click.echo()
        return tables

    def plan_add(self, source, target):
        count, tables = self.get_add_tables(source, target)
        _logger.debug('Add tables: {0}'.format(tables))
        for table in tables:
            click.secho('  + {0}'.format(table.table_id), fg='green')
            for line in dump_dataset(table).splitlines():
                click.echo('    {0}'.format(line))
            click.echo()
        return count

    def add(self, source, target):
        count, tables = self.get_add_tables(source, target)
        _logger.debug('Add tables: {0}'.format(tables))
        for table in tables:
            converted = BigQueryTable.to_table(self.dataset, table)
            click.secho('  Adding... {0}'.format(converted.path), fg='green')
            for line in dump_dataset(table).splitlines():
                click.echo('    {0}'.format(line))
            self.client.create_table(converted)
            click.echo()
        return count

    def plan_change(self, source, target):
        count, tables = self.get_change_tables(source, target)
        _logger.debug('Change tables: {0}'.format(tables))
        for table in tables:
            click.secho('  ~ {0}'.format(table.table_id), fg='yellow')
            old_table = next((s for s in source if s.table_id == table.table_id), None)
            for d in ndiff(old_table, table):
                click.secho('    {0}'.format(d), fg='yellow')
            click.echo()
        return count

    def change(self, source, target):
        count, tables = self.get_change_tables(source, target)
        _logger.debug('Change tables: {0}'.format(tables))
        for table in tables:
            converted = BigQueryTable.to_table(self.dataset, table)
            old_table = next((s for s in source if s.table_id == table.table_id), None)
            diff = ndiff(old_table, table)
            click.secho('  Changing... {0}'.format(converted.path), fg='yellow')
            for d in diff:
                click.secho('    {0}'.format(d), fg='yellow')
            old_labels = old_table.labels
            if old_labels:
                labels = converted.labels.copy()
                for k, v in iteritems(old_labels):
                    if k not in labels.keys():
                        labels[k] = None
                converted.labels = labels
            if table.schema != old_table.schema:
                self.migrate(old_table, table)
            self.client.update_table(converted, [
                'friendly_name',
                'description',
                'expires',
                'partitioning_type',
                'view_use_legacy_sql',
                'view_query',
                'labels',
            ])
            click.echo()
        return count

    def plan_destroy(self, source, target):
        count, tables = self.get_destroy_tables(source, target)
        _logger.debug('Destroy tables: {0}'.format(tables))
        for table in tables:
            click.secho('  - {0}'.format(table.table_id), fg='red')
            click.echo()
        return count

    def destroy(self, source, target):
        count, tables = self.get_destroy_tables(source, target)
        _logger.debug('Destroy tables: {0}'.format(tables))
        for table in tables:
            converted = BigQueryTable.to_table(self.dataset, table)
            click.secho('  Destroying... {0}'.format(converted.path), fg='red')
            self.client.delete_table(converted)
            click.echo()
        return count
