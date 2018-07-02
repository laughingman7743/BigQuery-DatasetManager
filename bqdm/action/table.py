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

import googleapiclient.discovery
from future.utils import iteritems
from google.cloud import bigquery
from google.cloud.bigquery.job import (CopyJobConfig, CreateDisposition, QueryJobConfig,
                                       WriteDisposition)
from google.cloud.exceptions import NotFound
from google.oauth2 import service_account

from bqdm.model.schema import BigQuerySchemaField
from bqdm.model.table import BigQueryTable
from bqdm.util import (dump, echo, echo_dump, echo_ndiff)

_logger = logging.getLogger(__name__)
_logger.addHandler(logging.StreamHandler(sys.stdout))
_logger.setLevel(logging.INFO)


class SchemaMigrationMode(Enum):

    SELECT_INSERT = 'select_insert'
    SELECT_INSERT_BACKUP = 'select_insert_backup'
    REPLACE = 'replace'
    REPLACE_BACKUP = 'replace_backup'
    DROP_CREATE = 'drop_create'
    DROP_CREATE_BACKUP = 'drop_create_backup'


class TableAction(object):

    def __init__(self, executor, dataset_id,
                 migration_mode=None, backup_dataset_id=None, project=None,
                 credential_file=None, no_color=False, debug=False):
        self._executor = executor
        credentials = None
        if credential_file:
            credentials = service_account.Credentials.from_service_account_file(credential_file)
        self._client = bigquery.Client(project, credentials)
        self._api_client = googleapiclient.discovery.build('bigquery', 'v2',
                                                           credentials=credentials)
        self._dataset_ref = self._client.dataset(dataset_id)
        if backup_dataset_id:
            self._backup_dataset_ref = self._client.dataset(backup_dataset_id)
        else:
            self._backup_dataset_ref = self._dataset_ref
        if migration_mode:
            self._migration_mode = SchemaMigrationMode(migration_mode)
        else:
            self._migration_mode = SchemaMigrationMode.SELECT_INSERT
        self.no_color = no_color
        if debug:
            _logger.setLevel(logging.DEBUG)

    @property
    def dataset_reference(self):
        return self._dataset_ref

    @property
    def dataset(self):
        return self._client.get_dataset(self._dataset_ref)

    @property
    def exists_dataset(self):
        try:
            self._client.get_dataset(self._dataset_ref)
            return True
        except NotFound:
            return False

    @property
    def backup_dataset_reference(self):
        return self._backup_dataset_ref

    @property
    def backup_dataset(self):
        return self._client.get_dataset(self._backup_dataset_ref)

    @property
    def exists_backup_dataset(self):
        try:
            self._client.get_dataset(self._backup_dataset_ref)
            return True
        except NotFound:
            return False

    @property
    def migration_mode(self):
        return self._migration_mode

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

    def migrate(self, source_table, target_table, prefix='    ', fg='yellow'):
        if self._migration_mode in [SchemaMigrationMode.SELECT_INSERT_BACKUP,
                                    SchemaMigrationMode.REPLACE_BACKUP,
                                    SchemaMigrationMode.DROP_CREATE_BACKUP]:
            self.backup(source_table.table_id)

        if self._migration_mode in [SchemaMigrationMode.SELECT_INSERT,
                                    SchemaMigrationMode.SELECT_INSERT_BACKUP]:
            query_field = TableAction.build_query_field(source_table.schema, target_table.schema)
            self.select_insert(target_table.table_id, target_table.table_id, query_field)
        elif self._migration_mode in [SchemaMigrationMode.REPLACE,
                                      SchemaMigrationMode.REPLACE_BACKUP]:
            tmp_table = self.create_temporary_table(target_table)
            query_field = TableAction.build_query_field(source_table.schema, target_table.schema)
            self.select_insert(source_table.table_id, tmp_table.table_id, query_field)
            self._destroy(target_table, prefix, fg)
            self._add(target_table, prefix, fg)
            query_field = TableAction.build_query_field(target_table.schema, target_table.schema)
            self.select_insert(tmp_table.table_id, target_table.table_id, query_field)
            self._destroy(tmp_table, prefix, fg)
        elif self._migration_mode in [SchemaMigrationMode.DROP_CREATE,
                                      SchemaMigrationMode.DROP_CREATE_BACKUP]:
            self._destroy(target_table, prefix, fg)
            self._add(target_table, prefix, fg)
        else:
            raise ValueError('Unknown migration mode.')

    def backup(self, source_table_id, prefix='    ', fg='yellow'):
        source_table = self.dataset.table(source_table_id)
        backup_table_id = 'backup_{source_table_id}_{timestamp}'.format(
            source_table_id=source_table_id,
            timestamp=datetime.utcnow().strftime('%Y%m%d%H%M%S%f'))
        backup_table = self.backup_dataset.table(backup_table_id)
        job_config = CopyJobConfig()
        job_config.create_disposition = CreateDisposition.CREATE_IF_NEEDED
        job = self._client.copy_table(source_table, backup_table, job_config=job_config)
        echo('Backing up... {0}'.format(job.job_id),
             prefix=prefix, fg=fg, no_color=self.no_color)
        job.result()
        assert job.state == 'DONE'
        error_result = job.error_result
        if error_result:
            raise RuntimeError(job.errors)

    def select_insert(self, source_table_id, destination_table_id, query_field,
                      prefix='    ', fg='yellow'):
        query = 'SELECT {query_field} FROM {dataset_id}.{source_table_id}'.format(
            query_field=query_field,
            dataset_id=self._dataset_ref.dataset_id,
            source_table_id=source_table_id)
        destination_table = self.dataset.table(destination_table_id)
        job_config = QueryJobConfig()
        job_config.use_legacy_sql = False
        job_config.use_query_cache = False
        job_config.write_disposition = WriteDisposition.WRITE_TRUNCATE
        job_config.destination = destination_table
        job = self._client.query(query, job_config)
        echo('Inserting... {0}'.format(job.job_id),
             prefix=prefix, fg=fg, no_color=self.no_color)
        echo('  {0}'.format(job.query),
             prefix=prefix, fg=fg, no_color=self.no_color)
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

    def update_schema_description(self, target_table):
        request = self._api_client.tables().patch(
            projectId=self._client.project,
            datasetId=self.dataset.dataset_id,
            tableId=target_table.table_id,
            body={
                'schema': target_table.schema_dict()
            }
        )
        request.execute()

    def create_temporary_table(self, model):
        tmp_table_model = copy.deepcopy(model)
        tmp_table_id = str(uuid.uuid4()).replace('-', '_')
        tmp_table_model.table_id = tmp_table_id
        tmp_table = BigQueryTable.to_table(self._dataset_ref, tmp_table_model)
        echo('    Temporary table creating... {0}'.format(tmp_table.path),
             fg='yellow', no_color=self.no_color)
        self._client.create_table(tmp_table)
        return tmp_table_model

    def get_table(self, table_id):
        table_ref = self.dataset.table(table_id)
        table = None
        try:
            table = self._client.get_table(table_ref)
            echo('Load table: ' + table.path)
            table = BigQueryTable.from_table(table)
        except NotFound:
            _logger.info('Table {0} is not found.'.format(table_id))
        return table

    def _list_tables(self):
        return self._client.list_tables(self.dataset)

    def list_tables(self):
        if not self.exists_dataset:
            return []

        fs = [self._executor.submit(self.get_table, t.table_id)
              for t in self._client.list_tables(self.dataset)]
        return fs

    def _export(self, output_dir, table_id):
        table = self.get_table(table_id)
        data = dump(table)
        _logger.debug(data)
        export_path = os.path.join(output_dir, '{0}.yml'.format(table.table_id))
        echo('Export table config: {0}'.format(export_path))
        with codecs.open(export_path, 'wb', 'utf-8') as f:
            f.write(data)
        return table

    def export(self, output_dir):
        output_dir = os.path.join(output_dir, self._dataset_ref.dataset_id)
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        tables = self._list_tables()
        fs = [self._executor.submit(self._export, output_dir, table.table_id)
              for table in tables]
        if not tables:
            keep_file = os.path.join(output_dir, '.gitkeep')
            if not os.path.exists(keep_file):
                open(keep_file, 'a').close()
        return fs

    def _add(self, model, prefix='  ', fg='green'):
        table = BigQueryTable.to_table(self._dataset_ref, model)
        echo('Adding... {0}'.format(table.path),
             prefix=prefix, fg=fg, no_color=self.no_color)
        echo_dump(model, prefix=prefix + '  ', fg=fg, no_color=self.no_color)
        self._client.create_table(table)
        echo()

    def plan_add(self, source, target, prefix='  ', fg='green'):
        count, tables = self.get_add_tables(source, target)
        _logger.debug('Add tables: {0}'.format(tables))
        for table in tables:
            echo('+ {0}'.format(table.table_id),
                 prefix=prefix, fg=fg, no_color=self.no_color)
            echo_dump(table, prefix=prefix + '  ', fg=fg, no_color=self.no_color)
            echo()
        return count

    def add(self, source, target, prefix='  ', fg='yellow'):
        count, tables = self.get_add_tables(source, target)
        _logger.debug('Add tables: {0}'.format(tables))
        fs = [self._executor.submit(self._add, t, prefix, fg) for t in tables]
        return count, fs

    def _change(self, source_model, target_model, prefix='  ', fg='yellow'):
        table = BigQueryTable.to_table(self._dataset_ref, target_model)
        echo('Changing... {0}'.format(table.path),
             prefix=prefix, fg=fg, no_color=self.no_color)
        echo_ndiff(source_model, target_model, prefix=prefix + '  ', fg=fg)
        source_labels = source_model.labels
        if source_labels:
            labels = table.labels.copy()
            for k, v in iteritems(source_labels):
                if k not in labels.keys():
                    labels[k] = None
            table.labels = labels
        if target_model.partitioning_type != source_model.partitioning_type:
            assert self._migration_mode not in [
                SchemaMigrationMode.SELECT_INSERT,
                SchemaMigrationMode.SELECT_INSERT_BACKUP],\
                'Migration mode: `{0}` not supported.'.format(self._migration_mode.value)
        target_schema_exclude_description = target_model.schema_exclude_description()
        source_schema_exclude_description = source_model.schema_exclude_description()
        if target_schema_exclude_description != source_schema_exclude_description or \
                target_model.partitioning_type != source_model.partitioning_type:
            self.migrate(source_model, target_model)
        if target_schema_exclude_description == source_schema_exclude_description and \
                target_model.schema != source_model.schema:
            self.update_schema_description(target_model)
        self._client.update_table(table, [
            'friendly_name',
            'description',
            'expires',
            'view_use_legacy_sql',
            'view_query',
            'labels',
        ])
        echo()

    def plan_change(self, source, target, prefix='  ', fg='yellow'):
        count, tables = self.get_change_tables(source, target)
        _logger.debug('Change tables: {0}'.format(tables))
        for table in tables:
            echo('~ {0}'.format(table.table_id),
                 prefix=prefix, fg=fg, no_color=self.no_color)
            source_table = next((s for s in source if s.table_id == table.table_id), None)
            echo_ndiff(source_table, table, prefix=prefix + ' ', fg=fg)
            echo()
        return count

    def change(self, source, target, prefix='  ', fg='yellow'):
        count, tables = self.get_change_tables(source, target)
        _logger.debug('Change tables: {0}'.format(tables))
        fs = [self._executor.submit(
            self._change, next((s for s in source if s.table_id == t.table_id), None),
            t, prefix, fg) for t in tables]
        return count, fs

    def _destroy(self, model, prefix='  ', fg='red'):
        table = BigQueryTable.to_table(self._dataset_ref, model)
        echo('Destroying... {0}'.format(table.path),
             prefix=prefix, fg=fg, no_color=self.no_color)
        self._client.delete_table(table)
        echo()

    def plan_destroy(self, source, target, prefix='  ', fg='red'):
        count, tables = self.get_destroy_tables(source, target)
        _logger.debug('Destroy tables: {0}'.format(tables))
        for table in tables:
            echo('- {0}'.format(table.table_id),
                 prefix=prefix, fg=fg, no_color=self.no_color)
            echo()
        return count

    def destroy(self, source, target, prefix='  ', fg='red'):
        count, tables = self.get_destroy_tables(source, target)
        _logger.debug('Destroy tables: {0}'.format(tables))
        fs = [self._executor.submit(self._destroy, t, prefix, fg) for t in tables]
        return count, fs
