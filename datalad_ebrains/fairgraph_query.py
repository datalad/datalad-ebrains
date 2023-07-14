
from functools import partial
import logging
import os
from pathlib import (
    Path,
    PurePosixPath,
)
import requests
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
        # picks up token from KG_AUTH_TOKEN ;
        # make sure to specify the url of the production server
        # (KGClient uses the pre-production server by default,
        # which can cause unexpected downtime, see
        # https://github.com/datalad/datalad-ebrains/issues/58)
        self.client = KGClient(host="core.kg.ebrains.eu")

    def bootstrap(self, from_id: str, dl_ds: Dataset, depth=None):
        kg_ds_uuid, kg_ds_versions = self.get_dataset_versions_from_id(
            from_id, depth=depth)
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

    def get_dataset_versions_from_id(self, id, depth=None):
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
        # robust handling of single-version datasets
        candidate_versions = (
            ds.versions
            if isinstance(ds.versions, list)
            else [ds.versions]
        )
        if depth:  # yes, we exclude zero too, makes no sense
            # the the last N
            candidate_versions = candidate_versions[-(depth):]

        versions = []
        for ver in candidate_versions:
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
        # EBRAINS uses different file repositories that need slightly
        # different handling
        dvr_url_p = urlparse(dvr.iri.value)
        # public data-proxy datasets
        if dvr_url_p.netloc == 'data-proxy.ebrains.eu' \
                and dvr_url_p.path.startswith('/api/v1/public/buckets/'):
            iter_files = partial(
                self.iter_files_dp,
                auth=False,
                get_fname=_get_fname_dataproxy_v1_bucket_public,
            )
        # private data-proxy datasets (e.g. human data gateway)
        elif dvr_url_p.netloc == 'data-proxy.ebrains.eu' \
                and dvr_url_p.path.startswith('/api/v1/buckets/'):
            iter_files = partial(
                self.iter_files_dp,
                auth=True,
                get_fname=_get_fname_dataproxy_v1_bucket_private,
            )
        elif dvr_url_p.netloc == 'object.cscs.ch' \
                and dvr_url_p.query.startswith('prefix='):
            # get the repos base url by removing the query string
            # input is like:
            # https://example.com/<basepath>?prefix=MPM-collections/13/
            # output is: https://example.com/<basepath>
            # the prefix is part of the file IRIs again
            dvr_prefix = dvr_url_p.query
            # this is a prefix and there are no other variables
            assert dvr_prefix.startswith('prefix=')
            assert dvr_prefix.count('=') == 1
            dvr_prefix = dvr_prefix[len('prefix='):]
            get_fname = partial(
                _get_fname_cscs_repo,
                # baseurl
                dvr_url_p._replace(query='').geturl(),
                # filerepo prefix
                dvr_prefix,
            )
            iter_files = partial(
                self.iter_files_kg,
                get_fname=get_fname,
            )
        else:
            raise NotImplementedError(
                f'Unrecognized file repository pointer {dvr.iri.value}')

        # must yield dict with keys
        # (url: str, name: str, md5sum: str, size: int)
        yield from iter_files(dvr)

    def iter_files_dp(self, dvr, auth, get_fname, chunk_size=10000):
        """Yield file records from a data proxy query"""
        bucket_url = f'https://data-proxy.ebrains.eu/api/v1/{dvr.name}'
        response = requests.get(
            # TODO handle properly
            f'{bucket_url}?limit=10000',
            # data proxy API will 400 if auth is sent for public resources
            headers={
                "Content-Type": "application/json",
                "Authorization": f'Bearer {os.environ["KG_AUTH_TOKEN"]}',
            } if auth else {},
        )
        response.raise_for_status()
        for f in response.json()['objects']:
            # f is a dict like:
            # {'hash': '16e1594b23e670086383ff7e7151d81a',
            #  'last_modified': '2023-02-06T15:06:59.748510',
            #  'bytes': 194037,
            #  'name': 'EBRAINS-DataDescriptor_JBA-v3.0.1.pdf',
            #  'content_type': 'application/pdf'}
            #
            # we need
            # (url: str, name: str, md5sum: str, size: int)
            yield dict(
                url=f'{bucket_url}/{f["name"]}',
                name=f['name'],
                md5sum=f['hash'],
                size=f['bytes'],
            )
        #yield from self.iter_files_kg(dvr, get_fname, chunk_size=chunk_size)

    # the chunk size is large, because the per-request latency costs
    # are enourmous
    # https://github.com/HumanBrainProject/fairgraph/issues/57
    def iter_files_kg(self, dvr, get_fname, chunk_size=10000):
        """Yield file records from a KG query"""
        cur_index = 0
        while True:
            batch = omcore.File.list(
                self.client,
                file_repository=dvr,
                size=chunk_size,
                from_index=cur_index)
            for f in batch:
                # we presently cannot understand non-md5 hashes
                assert f.hash.algorithm.lower() == 'md5'

                f_url = _file_iri_to_url(f.iri.value)
                fname = get_fname(f)
                yield dict(
                    url=f_url,
                    name=str(fname),
                    md5sum=f.hash.digest,
                    # assumed to be in bytes
                    size=f.storage_size.value,
                )
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


def _get_fname_dataproxy_v1_bucket_public(f):
    f_url_p = urlparse(f.iri.value)
    assert f_url_p.netloc == 'data-proxy.ebrains.eu'
    assert f_url_p.path.startswith('/api/v1/public/buckets/')
    path = PurePosixPath(f_url_p.path)
    # take everything past the bucket_id and turn into a Platform native path
    return Path(*path.parts[6:])


def _get_fname_dataproxy_v1_bucket_private(f):
    f_url_p = urlparse(f.iri.value)
    assert f_url_p.netloc == 'data-proxy.ebrains.eu'
    assert f_url_p.path.startswith('/api/v1/buckets/')
    path = PurePosixPath(f_url_p.path)
    # take everything past the bucket_id and turn into a Platform native path
    return Path(*path.parts[5:])


def _get_fname_cscs_repo(baseurl, prefix, f):
    f_url = f.iri.value
    # we presently have no better way to determine a relative file path
    # than to "subtract" the base URL
    assert f_url.startswith(baseurl)
    fname = f_url[len(baseurl):].lstrip('/')
    assert fname.startswith(prefix)
    # strip file repository prefix
    # TODO check https://github.com/datalad/datalad-ebrains/issues/39
    # if that is desirable
    # also strip any leading slash, any absolute path is invalid here
    fname = fname[len(prefix):].lstrip('/')
    # we have a relative posix path now
    fname = PurePosixPath(fname)
    # turn into a Platform native path
    fname = Path(*fname.parts)
    return fname


def _file_iri_to_url(iri):
    # the IRI is not a valid URL(?!), we must quote the path
    # to make it such
    f_url_p = urlparse(iri)
    return f_url_p._replace(path=quote(f_url_p.path)).geturl()
