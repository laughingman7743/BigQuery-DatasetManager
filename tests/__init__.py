# -*- coding: utf-8 -*-
import yaml

from bqdm.util import ordered_dict_constructor, str_representer
from bqdm.model import BigQueryDataset, BigQueryAccessGrant


yaml.add_representer(str, str_representer)
yaml.add_representer(BigQueryDataset, BigQueryDataset.represent)
yaml.add_representer(BigQueryAccessGrant, BigQueryAccessGrant.represent)
yaml.add_constructor('tag:yaml.org,2002:map', ordered_dict_constructor)
