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
    """Export a dataset from the EBRAINS Knowledge Graph as a DataLad dataset

    This command performs a series of queries against the EBRAINS Knowledge
    Graph (KG) to retrieve essential metadata on (versions of) an EBRAINS
    dataset. These metadata are used to build a Git-based representation
    of the dataset's evolution. This includes:

      - Any known ``DatasetVersion`` represented as a Git commit,
        i.e., a DataLad dataset version. The release date of the particular
        KG dataset version is recorded as the respective commit's date.

      - Any file that is part of a ``DatasetVersion`` registered as an
        annexed file in the respective DataLad dataset version, matching the
        directory/file name in the corresponding EBRAINS ``FileRepository``,
        with a URL suitable for file retrieval, and a checksum suitable
        for git-annex based content verification recorded for each file.

      - Each dataset version is (Git) tagged with the respective
        ``VersionIdentifier`` recorded in the EBRAINS KG.

      - Each dataset version carries the ``VersionInnovation`` recorded
        in the EBRAINS KG as its commit message.

    **Authentication**

    This command requires authentication with an EBRAINS user account.
    An access token has to be obtained and provided via the ``KG_AUTH_TOKEN``
    environment variable. Please see the `ebrain-authenticate` command
    for instructions on obtaining an access token.

    **Performance notes**

    For each considered ``DatasetVersion`` two principal queries are performed.
    The version query obtains essential metadata on that ``DatasetVersion``,
    the second query retrieve a list of files registered for that
    ``DatasetVersion``. The later query is slow, and takes ~30s per
    ``DatasetVersion``, regardless of the actual number of files registered.
    Consequently, cloning a dataset with any significant number of versions
    in the KG will take a considerable amount of time.
    This issue is known and tracked at
    https://github.com/HumanBrainProject/fairgraph/issues/57

    **Metadata validity**

    Metadata is always taken "as-is" from the EBRAINS KG. This can lead to
    unexpected results, in case metadata is are faulty. For example, it may
    happen that a newer dataset version has an assigned commit data that is
    older than its preceeding version.

    Moreover, the EBRAINS KG does not provide all essential metadata required
    for annotating a Git commit. For example, the agent identity associated
    with a ``DatasetVersion`` release is not available. This command
    unconditionally uses ``DataLad-EBRAINS exporter <ebrains@datalad.org>``
    as author and committer identity for this reason.

    **Reproducible dataset generation**

    Because no metadata modifications are performed and no local identity
    information is considered for generating a DataLad dataset, dataset
    cloning will yield reproducible results. In other words, running
    equivalent ``ebrains-clone`` commands, on different machines, at
    different times, by different users will yield the exact same DataLad
    datasets -- unless the metadata retrieved from the EBRAINS KG changes.
    Such changes can happen when metadata issues are corrected, or metadata
    available to a requesting user identity differs.

    Examples
    --------

    Clone the Julich-Brain Cytoarchitectonic Atlas at version 2.4 from the
    EBRAINS Knowledge Graph (the URL is taken directly from the EBRAINS
    data search web interface)::

      datalad ebrains-clone https://search.kg.ebrains.eu/instances/5249afa7-5e04-4ffd-8039-c3a9231f717c

    Clone the latest version of the Julich-Brain Cytoarchitectonic Atlas,
    including all prior versions recorded in the EBRAINS Knowledge Graph.
    Instead of a URL, here we only query for the respective UUID, which
    is identical to the "version overview" available via the EBRAINS
    web interface at
    https://search.kg.ebrains.eu/instances/5a16d948-8d1c-400c-b797-8a7ad29944b2::

      datalad ebrains-clone 5a16d948-8d1c-400c-b797-8a7ad29944b2

    UUIDs or URL can be used interchangably as an argument. In both cases,
    a UUID is extracted from the given argument.
    """

    _params_ = dict(
        source=Parameter(
            args=("source",),
            metavar='URL',
            doc="""URL including an ID of a dataset, or dataset version
            in the EBRAINS knowledge graph.
            (Such UUIDs can be found in the trailing part of the URL when
            looking at a dataset on https://search.kg.ebrains.eu).
            When an identifier/URL of a particular dataset version is provided
            all dataset versions preceeding this version are included in
            the generated dataset (including the identified version).
            If the URL/ID of a version-less dataset is given, all known
            versions for that dataset are included.""",
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
