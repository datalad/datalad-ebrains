"""DataLad demo extension"""

__docformat__ = 'restructuredtext'

# defines a datalad command suite
# this symbold must be indentified as a setuptools entrypoint
# to be found by datalad
command_suite = (
    # description of the command suite, displayed in cmdline help
    "HBP/EBRAINS support",
    [
        ('datalad_ebrains.authenticate', 'Authenticate',
         'ebrains-authenticate', 'ebrains_authenticate')
    ]
)

import datalad_ebrains.patches

from . import _version
__version__ = _version.get_versions()['version']
del _version
