
import logging
import os
from pathlib import (
    Path,
    PurePosixPath,
)
from unittest.mock import patch
from urllib.parse import (
    quote,
    urlparse,
)
import uuid

from fairgraph import KGClient
import fairgraph.openminds.core as omcore

from datalad_next.commands import get_status_dict
from datalad_next.exceptions import (
    CapturedException,
    IncompleteResultsError,
)
from datalad_next.datasets import Dataset
from datalad_next.utils import log_progress


lgr = logging.getLogger('datalad.ext.ebrains.fairgraph_query')


class FairGraphQuery:
    def __init__(self):
        # picks up token from KG_AUTH_TOKEN
        self.client = KGClient()

    def bootstrap(self, from_id: str, dl_ds: Dataset):
        kg_ds_uuid, kg_ds_versions = self.get_dataset_versions_from_id(from_id)
        # create datalad dataset
        # TODO support existing datasets
        try:
            ds = self.create_ds(dl_ds, kg_ds_versions[0], kg_ds_uuid)
        except IncompleteResultsError as e:
            # make sure to communicate the error outside
            yield from e.failed
            return

        # TODO support a starting version for the import
        # TODO maybe derive starting version automatically from a tag?
        log_id = f'ebrains-{from_id}'
        log_progress(
            lgr.info, log_id,
            'Querying dataset versions',
            unit=' Versions',
            label='Querying',
            total=len(kg_ds_versions),
        )
        try:
            for kg_dsver in kg_ds_versions:
                yield from self.import_datasetversion(ds, kg_dsver)
                log_progress(lgr.info, log_id,
                             'Completed version', update=1, increment=True)
        finally:
            log_progress(lgr.info, log_id, "Done querying knowledge graph")

    def create_ds(self, dl_ds, kg_ds_init_version, kg_ds_uuid):
        # create the dataset using the timestamp and agent of the
        # first version
        with patch.dict(
                os.environ,
                self.get_agent_info(kg_ds_init_version)):
            ds = dl_ds.create(result_renderer='disabled')
            # we create a reproducible dataset ID from the KG dataset ID
            # we are not reusing it directly, because we have two linked
            # but different objects
            ds.config.set(
                'datalad.dataset.id',
                # create a DNS namespace UUID from 'datalad.org'
                str(
                    uuid.uuid5(
                        uuid.uuid5(uuid.NAMESPACE_DNS, 'datalad.org'),
                        kg_ds_uuid,
                    )
                ),
                scope='branch',
            )
            # TODO establish meaningful gitattributes
            # e.g. README and LICENSE in Git
            ds.save(amend=True, result_renderer='disabled')
        return ds

    def get_dataset_versions_from_id(self, id):
        try:
            dv = omcore.DatasetVersion.from_id(id, self.client)
            target_version = dv.uuid
            # determine the Dataset from the DatasetVersion we got
            ds = omcore.Dataset.list(self.client, versions=dv)[0]
        except TypeError:
            # `id` might be the ID of a Dataset directly
            ds = omcore.Dataset.from_id(id, self.client)
            # all of them
            target_version = None
        versions = []
        # robust handling of single-version datasets
        for ver in (ds.versions
                    if isinstance(ds.versions, list)
                    else [ds.versions]):
            # resolving upfront might be suboptimal, but we know we need it
            # eventually, and it takes a fraction of the time to retrieve a
            # version-file-listing
            ver = ver.resolve(self.client)
            versions.append(ver)
            if ver.uuid == target_version:
                # do not go beyond the requested version
                break
        return ds.uuid, versions

    def import_datasetversion(self, ds, kg_dsver):
        self.clean_ds_worktree(ds)
        yield from self.import_files(ds, kg_dsver)
        self.import_metadata(ds, kg_dsver)
        yield from self.save_ds_version(ds, kg_dsver)

    def clean_ds_worktree(self, ds):
        dldir = ds.pathobj / '.datalad'
        exclude = (ds.pathobj / '.gitattributes',)
        # this is expensive, but theoretically there could be
        # numerous subdatasets, and they all need there content stripped
        for frec in ds.status(
                annex=None,
                untracked='all',
                recursive=True,
                eval_subdataset_state='full',
                result_renderer='disabled',
                return_type='generator',
        ):
            p = Path(frec['path'])
            if dldir in p.parents or p in exclude:
                # we are not wiping out any configuration
                continue
            Path(frec['path']).unlink()

    def import_files(self, ds, kg_dsver):
        try:
            yield from ds.addurls(
                # Turn query into an iterable of dicts for addurls
                urlfile=self.get_file_records(ds, kg_dsver),
                urlformat='{url}',
                filenameformat='{name}',
                # construct annex key from EBRAINS supplied info
                key='et:MD5-s{size}--{md5sum}',
                # we will have a better idea than "auto"
                exclude_autometa='*',
                # and here it would be
                #meta=(
                #    'ebrains_last_modified={last_modified}',
                #    'ebrain_last_modification_userid={last_modifier}',
                #),
                fast=True,
                save=False,
                result_renderer='disabled',
                return_type='generator',
            )
        except NotImplementedError as e:
            yield get_status_dict(
                status='impossible',
                action='ebrains-clone',
                exception=CapturedException(e),
            )

    def get_file_records(self, ds, kg_dsver):
        # the file repo IRI provides the reference for creating relative
        # file paths
        dvr = kg_dsver.repository.resolve(self.client)
        # get the repos base url by removing the query string
        # input is like: https://example.com/<basepath>?prefix=MPM-collections/13/
        # output is: https://example.com/<basepath>
        # the prefix is part of the file IRIs again
        dvr_url_p = urlparse(dvr.iri.value)
        dvr_prefix = dvr_url_p.query
        # this is a prefix and there are no other variables
        if not dvr_prefix.startswith('prefix='):
            raise NotImplementedError(
                f'Unrecognized file repository pointer {dvr.iri.value}')

        assert dvr_prefix.count('=') == 1
        dvr_prefix = dvr_prefix[len('prefix='):]
        dvr_baseurl = dvr_url_p._replace(query='').geturl()
        for f in self.iter_files(dvr):
            f_url = f.iri.value
            # the IRI is not a valid URL(?!), we must quote the path
            # to make it such
            f_url_p = urlparse(f_url)
            f_url = f_url_p._replace(path=quote(f_url_p.path)).geturl()
            # we presently no no better way to determine a relative file path
            # than to "subtract" the base URL
            assert f_url.startswith(dvr_baseurl)
            # we presently cannot understand non-md5 hashes
            assert f.hash.algorithm.lower() == 'md5'
            fname = f_url[len(dvr_baseurl):].lstrip('/')
            assert fname.startswith(dvr_prefix)
            # strip file repository prefix
            # TODO check https://github.com/datalad/datalad-ebrains/issues/39
            # if that is desirable
            fname = fname[len(dvr_prefix):]
            # we have a relative posix path now
            fname = PurePosixPath(fname)
            # turn into a Platform native path
            fname = Path(*fname.parts)
            yield dict(
                url=f_url,
                name=str(fname),
                md5sum=f.hash.digest,
                # assumed to be in bytes
                size=f.storage_size.value,
            )

    def iter_files(self, dvr, chunk_size=100):
        cur_index = 0
        while True:
            batch = omcore.File.list(
                self.client,
                file_repository=dvr,
                limit=chunk_size,
                from_index=cur_index)
            for f in batch:
                yield f
            if len(batch) < chunk_size:
                # there is no point in asking for another batch
                return
            cur_index += len(batch)

    def import_metadata(self, ds, kg_dsver):
        #(ds.pathobj / 'version').write_text(kg_dsver.version_identifier)
        pass

    def save_ds_version(self, ds, kg_dsver):
        with patch.dict(os.environ, self.get_agent_info(kg_dsver)):
            yield from ds.save(
                # TODO wrap the message?
                # TODO there is no meaningful subject line for the changelog
                # in this. Shall we have a standard subject that duplicates
                # version identifier or something else?
                message=kg_dsver.version_innovation,
                version_tag=kg_dsver.version_identifier,
                result_renderer='disabled',
                return_type='generator',
                on_failure='ignore',
            )

    def get_agent_info(self, kg_dsver):
        try:
            author_date = kg_dsver.release_date.isoformat()
        except AttributeError:
            # https://github.com/HumanBrainProject/fairgraph/issues/62
            author_date = ''
        author_name = 'DataLad-EBRAINS exporter'
        author_email = 'ebrains@datalad.org'
        return {
            'GIT_AUTHOR_NAME': author_name,
            'GIT_AUTHOR_EMAIL': author_email,
            'GIT_AUTHOR_DATE': author_date,
            # also apply to committer. this enables reproducible
            # dataset generation when nothing has changed in the KG
            'GIT_COMMITTER_NAME': author_name,
            'GIT_COMMITTER_EMAIL': author_email,
            'GIT_COMMITTER_DATE': author_date,
        }
