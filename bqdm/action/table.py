# -*- coding: utf-8 -*-
from __future__ import absolute_import

import codecs
import copy
import logging
import os
import sys
import uuid
from datetime import datetime
from enum import Enum

import click
from future.utils import iteritems
from google.cloud import bigquery
from google.cloud.bigquery.job import (CopyJobConfig, CreateDisposition, QueryJobConfig,
                                       WriteDisposition)
from google.oauth2 import service_account

from bqdm.model.schema import BigQuerySchemaField
from bqdm.model.table import BigQueryTable
from bqdm.util import dump, echo_dump, echo_ndiff

_logger = logging.getLogger(__name__)
_logger.addHandler(logging.StreamHandler(sys.stdout))
_logger.setLevel(logging.INFO)


class SchemaMigrationMode(Enum):

    SELECT_INSERT = 'select_insert'
    SELECT_INSERT_BACKUP = 'select_insert_backup'
    REPLACE = 'replace'
    REPLACE_BACKUP = 'replace_backup'
    DROP_CREATE = 'drop_create'


class TableAction(object):

    def __init__(self, dataset_id, migration_mode=SchemaMigrationMode.SELECT_INSERT,
                 backup_dataset_id=None, project=None, credential_file=None, debug=False):
        credentials = None
        if credential_file:
            credentials = service_account.Credentials.from_service_account_file(credential_file)
        self.client = bigquery.Client(project, credentials)
        dataset_ref = self.client.dataset(dataset_id)
        self.dataset = self.client.get_dataset(dataset_ref)
        if backup_dataset_id:
            backup_dataset_ref = self.client.dataset(backup_dataset_id)
            self.backup_dataset = self.client.get_dataset(backup_dataset_ref)
        else:
            self.backup_dataset = self.dataset
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
        if self.migration_mode in [SchemaMigrationMode.SELECT_INSERT_BACKUP,
                                   SchemaMigrationMode.REPLACE_BACKUP]:
            self.backup(source_table.table_id)

        if self.migration_mode in [SchemaMigrationMode.SELECT_INSERT,
                                   SchemaMigrationMode.SELECT_INSERT_BACKUP]:
            query_field = TableAction.build_query_field(source_table.schema, target_table.schema)
            self.select_insert(target_table.table_id, target_table.table_id, query_field)
        elif self.migration_mode in [SchemaMigrationMode.REPLACE,
                                     SchemaMigrationMode.REPLACE_BACKUP]:
            tmp_table = self.create_temporary_table(target_table)
            query_field = TableAction.build_query_field(source_table.schema, target_table.schema)
            self.select_insert(source_table.table_id, tmp_table.table_id, query_field)
            self._destroy(target_table)
            self._add(target_table)
            self.select_insert(tmp_table.table_id, target_table.table_id, '*')
            self._destroy(tmp_table)
        elif self.migration_mode in [SchemaMigrationMode.DROP_CREATE]:
            self._destroy(target_table)
            self._add(target_table)
        else:
            raise ValueError('Unknown migration mode.')

    def backup(self, source_table_id):
        source_table = self.dataset.table(source_table_id)
        backup_table_id = 'backup_{source_table_id}_{timestamp}'.format(
            source_table_id=source_table_id,
            timestamp=datetime.utcnow().strftime('%Y%m%d%H%M%S%f'))
        backup_table = self.backup_dataset.table(backup_table_id)
        job_config = CopyJobConfig()
        job_config.create_disposition = CreateDisposition.CREATE_IF_NEEDED
        job = self.client.copy_table(source_table, backup_table, job_config=job_config)
        click.secho('    Backing up... {0}'.format(job.job_id), fg='yellow')
        job.result()
        assert job.state == 'DONE'
        error_result = job.error_result
        if error_result:
            raise RuntimeError(job.errors)

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
        job = self.client.query(query, job_config)
        click.secho('    Inserting... {0}'.format(job.job_id), fg='yellow')
        click.secho('      {0}'.format(job.query), fg='yellow')
        job.result()
        assert job.state == 'DONE'
        error_result = job.error_result
        if error_result:
            raise RuntimeError(job.errors)

    @staticmethod
    def build_query_field(source_schema, target_schema, prefix=None):
        query_fields = []
        for target in target_schema:
            source = next((s for s in source_schema if s.name == target.name), None)
            if source:
                if target.field_type == 'RECORD':
                    field_prefix = '{0}.{1}'.format(
                        prefix, target.name) if prefix else target.name
                    fields = TableAction.build_query_field(source.fields, target.fields,
                                                           field_prefix)
                    query_fields.append('struct({fields}) AS {alias}'.format(
                        fields=fields, alias=target.name))
                else:
                    name = '{0}.{1}'.format(prefix, target.name) if prefix else target.name
                    field_type = BigQuerySchemaField.normalize_field_type(target.field_type)
                    query_fields.append('cast({name} AS {type}) AS {alias}'.format(
                        name=name, type=field_type, alias=target.name))
            else:
                if target.field_type == 'RECORD':
                    field_prefix = '{0}.{1}'.format(
                        prefix, target.name) if prefix else target.name
                    fields = TableAction.build_query_field((), target.fields,
                                                           field_prefix)
                    query_fields.append('struct({fields}) AS {alias}'.format(
                        fields=fields, alias=target.name))
                else:
                    query_fields.append('null AS {alias}'.format(alias=target.name))
        return ', '.join(query_fields)

    def create_temporary_table(self, model):
        tmp_table_model = copy.deepcopy(model)
        tmp_table_id = str(uuid.uuid4()).replace('-', '_')
        tmp_table_model.table_id = tmp_table_id
        table = BigQueryTable.to_table(self.dataset.reference, model)
        click.secho('    Temporary table creating... {0}'.format(table.path), fg='yellow')
        self.client.create_table(table)
        return tmp_table_model

    def list_tables(self):
        tables = []
        for table in self.client.list_tables(self.dataset):
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
            data = dump(BigQueryTable.from_table(table))
            _logger.debug(data)
            export_path = os.path.join(output_dir, '{0}.yml'.format(table.table_id))
            click.echo('Export: {0}'.format(export_path))
            with codecs.open(export_path, 'wb', 'utf-8') as f:
                f.write(data)
        click.echo()
        return tables

    def _add(self, model):
        table = BigQueryTable.to_table(self.dataset.reference, model)
        click.secho('  Adding... {0}'.format(table.path), fg='green')
        echo_dump(model)
        self.client.create_table(table)
        click.echo()

    def plan_add(self, source, target):
        count, tables = self.get_add_tables(source, target)
        _logger.debug('Add tables: {0}'.format(tables))
        for table in tables:
            click.secho('  + {0}'.format(table.table_id), fg='green')
            echo_dump(table)
            click.echo()
        return count

    def add(self, source, target):
        count, tables = self.get_add_tables(source, target)
        _logger.debug('Add tables: {0}'.format(tables))
        for table in tables:
            self._add(table)
        return count

    def _change(self, source_model, target_model):
        table = BigQueryTable.to_table(self.dataset.reference, target_model)
        click.secho('  Changing... {0}'.format(table.path), fg='yellow')
        echo_ndiff(source_model, target_model)
        source_labels = source_model.labels
        if source_labels:
            labels = table.labels.copy()
            for k, v in iteritems(source_labels):
                if k not in labels.keys():
                    labels[k] = None
            table.labels = labels
        # TODO location change & partitioning_type change
        if target_model.schema != source_model.schema:
            self.migrate(source_model, target_model)
        self.client.update_table(table, [
            'friendly_name',
            'description',
            'expires',
            'view_use_legacy_sql',
            'view_query',
            'labels',
        ])
        click.echo()

    def plan_change(self, source, target):
        count, tables = self.get_change_tables(source, target)
        _logger.debug('Change tables: {0}'.format(tables))
        for table in tables:
            click.secho('  ~ {0}'.format(table.table_id), fg='yellow')
            source_table = next((s for s in source if s.table_id == table.table_id), None)
            echo_ndiff(source_table, table)
            click.echo()
        return count

    def change(self, source, target):
        count, tables = self.get_change_tables(source, target)
        _logger.debug('Change tables: {0}'.format(tables))
        for table in tables:
            source_table = next((s for s in source if s.table_id == table.table_id), None)
            self._change(source_table, table)
        return count

    def _destroy(self, model):
        table = BigQueryTable.to_table(self.dataset.reference, model)
        click.secho('  Destroying... {0}'.format(table.path), fg='red')
        self.client.delete_table(table)
        click.echo()

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
            self._destroy(table)
        return count
