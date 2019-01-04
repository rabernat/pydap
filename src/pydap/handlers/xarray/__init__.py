"""pydap handler for xarray."""

import os
import re
import time
from stat import ST_MTIME
from email.utils import formatdate
import numpy as np

from pkg_resources import get_distribution

from ...model import DatasetType, GridType, BaseType
from ..lib import BaseHandler
from ...exceptions import OpenFileError
from ...pycompat import suppress

from collections import OrderedDict

import xarray as xr

class XarrayHandler(BaseHandler):

    """An xarray handler for NetCDF files.
    """

    __version__ = get_distribution("pydap").version
    extensions = re.compile(r"^.*\.(nc|cdf)$", re.IGNORECASE)

    def __init__(self, filepath):
        BaseHandler.__init__(self)

        self.filepath = filepath
        try:
            with xr.open_dataset(self.filepath) as source:
                self.additional_headers.append(('Last-modified',
                                               (formatdate(
                                                time.mktime(
                                                    time.localtime(
                                                        os.stat(filepath)
                                                        [ST_MTIME])
                                                        )))))

                # build dataset
                name = os.path.split(filepath)[1]
                self.dataset = DatasetType(name, attributes=dict(
                                                      NC_GLOBAL=source.attrs))

                ## How to handle unlimited dims with xarray?
                #for dim in dims:
                #    if dims[dim] is None:
                #        self.dataset.attributes['DODS_EXTRA'] = {
                #            'Unlimited_Dimension': dim,
                #        }
                #        break

                # add grids
                for key, var in source.data_vars.items():
                    v = GridType(key)
                    v[key] = BaseType(key, var.values, dimensions=var.dims,
                                      **var.attrs)
                    for d in var.dims:
                        v[d] = BaseType(d, var[d].values)
                    self.dataset[key] = v
                # ensure all dims are stored in ds
                for d in source.coords:
                    self.dataset[d] = BaseType(d, source[d].values,
                                               dimensions=(d, ),
                                               **source[d].attrs)

        except Exception as exc:
            raise
            message = 'Unable to open file %s: %s' % (filepath, exc)
            raise OpenFileError(message)


if __name__ == "__main__":
    import sys
    from werkzeug.serving import run_simple

    application = XarrayHandler(sys.argv[1])
    run_simple('localhost', 8001, application, use_reloader=True)
