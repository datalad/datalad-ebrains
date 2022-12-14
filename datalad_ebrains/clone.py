import logging
from pathlib import Path
import re
import warnings

from datalad_next.commands import (
    ValidatedInterface,
    Parameter,
    build_doc,
    eval_results,
)
from datalad_next.constraints import (
    EnsurePath,
    EnsureURL,
)
from datalad_next.constraints.dataset import EnsureDataset
from datalad_next.datasets import datasetmethod

from datalad_ebrains.fairgraph_query import FairGraphQuery


lgr = logging.getLogger('datalad.ext.ebrains.clone')

uuid_regex = \
    '^.*([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}).*$'


@build_doc
class Clone(ValidatedInterface):
    """Export any dataset from the EBRAINS Knowledge Graph (KG) as a dataset

    *Obtain authorization for performing KG queries*

    These instructions reflect the current (development) setup. Eventually,
    KG queries as needed here should become accessible without dedicated
    authorization.

    1. Register and request authorization https://kg.ebrains.eu/develop.html
    2. Get an authentication token from
       https://nexus-iam.humanbrainproject.org/v0/oauth2/authorize
       This token is only valid for a short period of time (<24h?).
    3. Define EBRAINS_TOKEN environment variable with the token.
    """

    _params_ = dict(
        source=Parameter(
            args=("source",),
            metavar='URL',
            doc="""URL including an ID of a dataset in the knowledge graph.
            Such UUIDs can be found in the trailing part of the URL when
            looking at a dataset on https://search.kg.ebrains.eu""",
        ),
        path=Parameter(
            args=("path",),
            metavar='PATH',
            nargs="?",
            doc="""path to clone into.  If no `path` is provided the
            destination will be the current working directory."""),
        dataset=Parameter(
            args=("-d", "--dataset"),
            doc=""""Dataset to create""",
        ),
    )

    _validators_ = dict(
        # must be a URL with any UUID in the string
        source=EnsureURL(match=uuid_regex),
        # non-existing or empty, EnsurePath cannot express that yet
        path=EnsurePath(),
        dataset=EnsureDataset(),
    )

    @staticmethod
    @datasetmethod(name='ebrains_clone')
    @eval_results
    def __call__(source, path=None, *, dataset=None):
        source_match = re.match(uuid_regex, source)
        ebrains_id = source_match.group(1)
        # this is ensured by the constraint
        assert ebrains_id

        target_ds_param = EnsureDataset(installed=False)(path or Path.cwd())

        fq = FairGraphQuery()

        res_kwargs = dict(
            logger=lgr,
            action='ebrains_clone',
            ds=target_ds_param.ds,
        )

        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            for res in fq.bootstrap(ebrains_id, target_ds_param.ds):
                yield dict(
                    res_kwargs,
                    **res,
                )
