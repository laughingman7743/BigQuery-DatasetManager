#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import absolute_import
import codecs
import logging
import os
import sys

import click
import yaml
from google.cloud import bigquery
from past.types import unicode

import bqdm.message as msg
from bqdm import CONTEXT_SETTINGS
from bqdm.model import BigQueryDataset, BigQueryAccessGrant
from bqdm.util import (get_add_datasets, get_change_datasets, get_destroy_datasets,
                       get_intersection_datasets, dump_dataset, load_bigquery_datasets,
                       load_local_datasets, ndiff, ordered_dict_constructor, str_representer)


_logger = logging.getLogger(__name__)
_logger.addHandler(logging.StreamHandler(sys.stdout))


yaml.add_representer(str, str_representer)
yaml.add_representer(unicode, str_representer)
yaml.add_representer(BigQueryDataset, BigQueryDataset.represent)
yaml.add_representer(BigQueryAccessGrant, BigQueryAccessGrant.represent)
yaml.add_constructor('tag:yaml.org,2002:map', ordered_dict_constructor)


@click.group(context_settings=CONTEXT_SETTINGS)
@click.option('--credential_file', '-c', type=click.Path(exists=True), required=False,
              help=msg.HELP_CREDENTIAL_FILE)
@click.option('--debug', is_flag=True, default=False, help=msg.HELP_DEBUG)
@click.pass_context
def cli(ctx, credential_file, debug):
    ctx.obj = dict()
    ctx.obj['credential_file'] = credential_file
    if credential_file:
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credential_file
    if debug:
        _logger.setLevel(logging.DEBUG)
    else:
        _logger.setLevel(logging.INFO)


@cli.command(help=msg.HELP_COMMAND_EXPORT)
@click.option('--output_dir', '-o', type=str, required=True, help=msg.HELP_OUTPUT_DIR)
@click.pass_context
def export(ctx, output_dir):
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    client = bigquery.Client()
    datasets = client.list_datasets()
    for dataset in datasets:
        dataset.reload()
        click.echo('Export: {0}'.format(dataset.path))
        data = dump_dataset(BigQueryDataset.from_dataset(dataset))
        _logger.debug(data)
        with codecs.open(os.path.join(output_dir, '{0}.yml'.format(dataset.name)),
                         'wb', 'utf-8') as f:
            f.write(data)


@cli.command(help=msg.HELP_COMMAND_PLAN)
@click.option('--conf_dir', '-d', type=click.Path(exists=True, file_okay=False), required=True,
              help=msg.HELP_CONF_DIR)
@click.option('--detailed_exitcode', is_flag=True, default=False, help=msg.HELP_DETAILED_EXIT_CODE)
@click.pass_context
def plan(ctx, conf_dir, detailed_exitcode):
    client = bigquery.Client()

    old_datasets = load_bigquery_datasets(client)
    new_datasets = load_local_datasets(conf_dir)

    # echo header
    click.echo(msg.MESSAGE_PLAN_HEADER)

    # add
    add_count, add_datasets = get_add_datasets(old_datasets, new_datasets)
    _logger.debug('Add datasets: {0}'.format(add_datasets))
    for dataset in add_datasets:
        click.secho('  + {0}'.format(dataset.name), fg='green')
        for line in dump_dataset(dataset).splitlines():
            click.echo('    {0}'.format(line))
        click.echo()

    # change
    change_count, change_datasets = get_change_datasets(old_datasets, new_datasets)
    _logger.debug('Change datasets: {0}'.format(change_datasets))
    for dataset in change_datasets:
        click.secho('  ~ {0}'.format(dataset.name), fg='yellow')
        old_dataset = next((o for o in old_datasets if o.name == dataset.name), None)
        for d in ndiff(old_dataset, dataset):
            click.echo('    {0}'.format(d))
        click.echo()

    # destroy
    destroy_count, destroy_datasets = get_destroy_datasets(old_datasets, new_datasets)
    _logger.debug('Destroy datasets: {0}'.format(destroy_datasets))
    for dataset in destroy_datasets:
        click.secho('  - {0}'.format(dataset.name), fg='red')
        click.echo()

    # echo summary
    if add_count == 0 and change_count == 0 and destroy_count == 0:
        click.secho(msg.MESSAGE_SUMMARY_NO_CHANGE)
        click.echo()
    else:
        click.echo(msg.MESSAGE_PLAN_SUMMARY.format(add_count, change_count, destroy_count))
        click.echo()
        if detailed_exitcode:
            sys.exit(2)


@cli.command(help=msg.HELP_COMMAND_APPLY)
@click.option('--conf_dir', '-d', type=click.Path(exists=True, file_okay=False), required=True,
              help=msg.HELP_CONF_DIR)
@click.pass_context
def apply(ctx, conf_dir):
    client = bigquery.Client()

    old_datasets = load_bigquery_datasets(client)
    new_datasets = load_local_datasets(conf_dir)

    # add
    add_count, add_datasets = get_add_datasets(old_datasets, new_datasets)
    _logger.debug('Add datasets: {0}'.format(add_datasets))
    for dataset in add_datasets:
        converted = BigQueryDataset.to_dataset(client, dataset)
        click.secho('  Adding... {0}'.format(converted.path), fg='green')
        for line in dump_dataset(dataset).splitlines():
            click.echo('    {0}'.format(line))
        converted.create()
        click.echo()

    # change
    change_count, change_datasets = get_change_datasets(old_datasets, new_datasets)
    _logger.debug('Change datasets: {0}'.format(change_datasets))
    for dataset in change_datasets:
        converted = BigQueryDataset.to_dataset(client, dataset)
        old_dataset = next((o for o in old_datasets if o.name == dataset.name), None)
        diff = ndiff(old_dataset, dataset)
        click.secho('  Changing... {0}'.format(converted.path), fg='yellow')
        for d in diff:
            click.echo('    {0}'.format(d))
        converted.update()
        click.echo()

    # destroy
    destroy_count, destroy_datasets = get_destroy_datasets(old_datasets, new_datasets)
    _logger.debug('Destroy datasets: {0}'.format(destroy_datasets))
    for dataset in destroy_datasets:
        converted = BigQueryDataset.to_dataset(client, dataset)
        click.secho('  Destroying... {0}'.format(converted.path), fg='red')
        converted.delete()
        click.echo()

    # echo summary
    if add_count == 0 and change_count == 0 and destroy_count == 0:
        click.secho(msg.MESSAGE_SUMMARY_NO_CHANGE)
        click.echo()
    else:
        click.secho(msg.MESSAGE_APPLY_SUMMARY.format(add_count, change_count, destroy_count))
        click.echo()


@cli.group(help=msg.HELP_COMMAND_DESTROY)
@click.pass_context
def destroy(ctx):
    pass


@destroy.command('plan', help=msg.HELP_COMMAND_PLAN_DESTROY)
@click.option('--conf_dir', '-d', type=click.Path(exists=True, file_okay=False), required=True,
              help=msg.HELP_CONF_DIR)
@click.option('--detailed_exitcode', is_flag=True, default=False, help=msg.HELP_DETAILED_EXIT_CODE)
@click.pass_context
def plan_destroy(ctx, conf_dir, detailed_exitcode):
    client = bigquery.Client()

    old_datasets = load_bigquery_datasets(client)
    new_datasets = load_local_datasets(conf_dir)

    # echo header
    click.echo(msg.MESSAGE_PLAN_HEADER)

    # destroy
    destroy_count, destroy_datasets = get_intersection_datasets(new_datasets, old_datasets)
    _logger.debug('Destroy datasets: {0}'.format(destroy_datasets))
    for dataset in destroy_datasets:
        click.secho('  - {0}'.format(dataset.name), fg='red')
        click.echo()

    # echo summary
    if destroy_count == 0:
        click.secho(msg.MESSAGE_SUMMARY_NO_CHANGE)
        click.echo()
    else:
        click.echo(msg.MESSAGE_PLAN_DESTROY_SUMMARY.format(destroy_count))
        click.echo()
        if detailed_exitcode:
            sys.exit(2)


@destroy.command('apply', help=msg.HELP_COMMAND_APPLY_DESTROY)
@click.option('--conf_dir', '-d', type=click.Path(exists=True, file_okay=False), required=True,
              help=msg.HELP_CONF_DIR)
@click.pass_context
def apply_destroy(ctx, conf_dir):
    client = bigquery.Client()

    old_datasets = load_bigquery_datasets(client)
    new_datasets = load_local_datasets(conf_dir)

    # destroy
    destroy_count, destroy_datasets = get_intersection_datasets(new_datasets, old_datasets)
    _logger.debug('Destroy datasets: {0}'.format(destroy_datasets))
    for dataset in destroy_datasets:
        converted = BigQueryDataset.to_dataset(client, dataset)
        click.secho('  Destroying... {0}'.format(dataset.name), fg='red')
        converted.delete()
        click.echo()

    # echo summary
    if destroy_count == 0:
        click.secho(msg.MESSAGE_SUMMARY_NO_CHANGE)
        click.echo()
    else:
        click.echo(msg.MESSAGE_APPLY_DESTROY_SUMMARY.format(destroy_count))
        click.echo()


if __name__ == '__main__':
    cli()
