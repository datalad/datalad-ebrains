import logging
from collections import OrderedDict

from datalad.distribution.dataset import datasetmethod
from datalad.interface.base import (
    Interface,
    build_doc,
)
from datalad.interface.results import get_status_dict
from datalad.interface.utils import eval_results
from datalad.support.param import Parameter
from datalad.support.constraints import (
    Constraint,
    EnsureNone,
)
from datalad.distribution.dataset import (
    EnsureDataset,
    require_dataset,
)
from datalad_ebrains.kg_query import (
    KGQueryException,
    get_annex_key_records,
    get_token,
    get_kgds_parent_id,
    query_kg4dataset,
)

lgr = logging.getLogger('datalad.ebrains.kg2ds')


class EnsureUUID(Constraint):
    """Ensure that input is a valid UUID.
    """
    def __call__(self, value):
        from uuid import UUID
        return str(UUID(value))

    def short_description(self):
        return 'UUID'

    def long_description(self):
        return "value must be a valid UUID"


@build_doc
class KnowledgeGraph2Dataset(Interface):
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
        kgid=Parameter(
            args=("kgid",),
            metavar='KGID',
            doc="""ID of a dataset in the knowledge graph. This is a UUID
            that is the trailing part of the URL when looking at a dataset
            at http://kg.ebrains.eu""",
            constraints=EnsureUUID()),
        dataset=Parameter(
            args=("-d", "--dataset"),
            doc=""""Dataset to create""",
            constraints=EnsureDataset() | EnsureNone()
        ),
    )

    @staticmethod
    @datasetmethod(name='ebrains_kg2ds')
    @eval_results
    def __call__(kgid, dataset=None):
        ds = require_dataset(
            dataset, check_installed=True,
            purpose='exporting EBRAINS knowledge graph dataset')

        res_kwargs = dict(
            logger=lgr,
            action='kg2ds',
            ds=ds,
        )

        auth_token = get_token()

        revisions = OrderedDict()
        # TODO query for multiple revisions should be optional
        while kgid is not None:
            try:
                qres = query_kg4dataset(auth_token, kgid)
            except KGQueryException as e:
                yield get_status_dict(
                    status='error',
                    message=(
                        'knowlegde graph query %s (query ID: %s)',
                        str(e), kgid
                    ),
                    **res_kwargs
                )
                return
            revisions[kgid] = qres
            lgr.info("Found revision %s", kgid)
            # look for a parent revision
            kgid = get_kgds_parent_id(qres)
            if kgid in revisions:
                lgr.warn("Circular revisions detected, stopping query")
                kgid = None
            # TODO query for multiple revisions should be optional
            break

        if not revisions:
            yield get_status_dict(
                status='error',
                message='No revision to build a dataset from',
                **res_kwargs)
            return

        # XXX dump query for dev purposes for now
        from pprint import pprint
        pprint(revisions)

        # only a single revision for now
        kgdsid, kgdsrec = revisions.popitem()

        ds.addurls(
            # Turn query into an iterable of dicts for addurls
            urlfile=get_annex_key_records(kgdsrec),
            urlformat='{url}',
            filenameformat='{name}',
            # construct annex key from EBRAINS supplied info
            key='et:MD5-s{size}--{md5sum}',
            # we have a better idea than "auto"
            exclude_autometa='*',
            # and here it is
            meta=(
                'content_type={content_type}',
                'ebrains_last_modified={last_modified}',
                'ebrain_last_modification_userid={last_modifier}',
            ),
            fast=True,
            save=False,
        )
        # TODO?
        # - adjust timestamps to the last modification
        # - adjust author info to EBRAINS user (in contrast to committer)
        # - auto-build README

        yield from ds.save(
            # make parameter?
            # we could face a dataset that is scattered across various
            # subdatasets
            recursive=True,
            # TODO make pretty
            message=kgdsid,
        )

        yield get_status_dict(
            status='ok',
            **res_kwargs)
