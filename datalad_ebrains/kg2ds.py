from tempfile import NamedTemporaryFile

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
from datalad.support.json_py import (
    jsondump,
    jsonload,
)


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
    """Export any dataset from the EBRAINS Knowledge Graph as a DataLad dataset

    some
    """

    _params_ = dict(
        kgid=Parameter(
            args=("kgid",),
            metavar='KGID',
            doc="""ID of a dataset in the knowledge graph. This is a UUID
            that is the trailing part of the URL when looking at a dataset
            at http://kg,ebrains.eu""",
            ),
            #constraints=EnsureUUID()),
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

        # TODO later we will perform an actual query here, but for now load
        # a prefetched and stored response from a file
        response = jsonload(open(kgid))
        results = response.get('results', [])
        if not results:
            yield get_status_dict(
                action='kg2ds',
                status='impossible',
                message=('knowledge graph query for dataset ID %s yielded no records',
                         kgid),
            )
            return
        if len(results) > 1:
            yield get_status_dict(
                action='kg2ds',
                status='error',
                message='knowledge graph query yielded more than one result record',
            )
            return
        file_specs = results[0].get('https://schema.hbp.eu/myQuery/v1.0.0', [])
        if not results:
            yield get_status_dict(
                action='kg2ds',
                status='impossible',
                message=('knowledge graph query for dataset ID %s yielded no files',
                         kgid),
            )
            return

        # construct a file list (with metadata) suitable for addurls
        file_list = []
        for spec in file_specs:
            url_spec = {}
            for s, d in (('https://schema.hbp.eu/myQuery/relativeUrl', 'kg_id'),
                         ('https://schema.hbp.eu/myQuery/last_modified', 'last_modified'),
                         ('https://schema.hbp.eu/myQuery/absolute_path', 'url'),
                         ('https://schema.hbp.eu/myQuery/relative_path', 'path')):
                if s in spec:
                    url_spec[d] = spec[s]
            if url_spec:
                file_list.append(url_spec)
            else:
                pass
                # TODO log skipped item

        # TODO URLs cannot be accessed as such, but need prior authorization
        # figure out how that must be done
        with NamedTemporaryFile('w') as tmp_listfile:
            jsondump(file_list, tmp_listfile)
            tmp_listfile.seek(0)
            from datalad.api import addurls
            addurls(
                dataset=dataset or '',
                urlfile=tmp_listfile.name,
                urlformat='{url}',
                filenameformat='{path}',
                input_type='json',
                # TODO
                fast=True,
            )

        # TODO? adjust timestamps to the last modification

        yield None
        #yield get_status_dict(
        #    action='kg2ds',
        #    path=abspath(curdir),
        #    status='ok')

