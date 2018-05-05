# -*- coding: utf-8 -*-
from __future__ import absolute_import

import functools
import os

from google.cloud import bigquery


class Env(object):

    def __init__(self):
        self.credential = os.getenv('GOOGLE_APPLICATION_CREDENTIALS', None)
        assert self.credential, \
            'Required environment variable `GOOGLE_APPLICATION_CREDENTIALS` not found.'
        self.project = os.getenv('GOOGLE_CLOUD_PROJECT', None)
        assert self.project, \
            'Required environment variable `GOOGLE_CLOUD_PROJECT` not found.'


def with_client(fn):
    @functools.wraps(fn)
    def wrapped_fn(self, *args, **kwargs):
        client = bigquery.Client()
        fn(self, client, *args, **kwargs)
    return wrapped_fn
