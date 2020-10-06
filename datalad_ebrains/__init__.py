"""DataLad demo extension"""

__docformat__ = 'restructuredtext'

# defines a datalad command suite
# this symbold must be indentified as a setuptools entrypoint
# to be found by datalad
command_suite = (
    # description of the command suite, displayed in cmdline help
    "HBP/EBRAINS support",
    [
        # specification of a command, any number of commands can be defined
        (
            # importable module that contains the command implementation
            'datalad_ebrains.kg2ds',
            # name of the command class implementation in above module
            'KnowledgeGraph2Dataset',
            # optional name of the command in the cmdline API
            'ebrains-kg2ds',
            # optional name of the command in the Python API
            'ebrains_kg2ds'
        ),
    ]
)


from datalad import setup_package
from datalad import teardown_package

from ._version import get_versions
__version__ = get_versions()['version']
del get_versions
