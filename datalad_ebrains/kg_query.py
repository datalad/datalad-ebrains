import logging
from os import environ
import requests
import json

from datalad.downloaders.credentials import Token
from datalad import ui

lgr = logging.getLogger('datalad.ebrains.kg_query')

# use query endpoint of a stored query for datasets
# see tools/kg_query.py
# TODO this is a temporary stored query, finalize!
kg_dataset_query = \
    "https://kg.humanbrainproject.eu/query/minds/core/dataset/v1.0.0/mih_ds"

ds_id_key = 'https://schema.hbp.eu/myQuery/identifier'
ds_revision_key = 'https://schema.hbp.eu/myQuery/wasRevisionOf'
ds_filelist_key = 'https://schema.hbp.eu/myQuery/v1.0.0'
type_key = 'https://schema.hbp.eu/myQuery/@type'


def get_auth_headers(token):
    """Get token-based auth headers for a KG HTPP request

    Parameters
    ----------
    token: str
      EBRAINS Auth-token

    Returns
    -------
    dict
      Auth headers as a dict, fit for requests.get()
    """
    return {
        'Content-Type': 'application/json',
        'Authorization': 'Bearer {}'.format(token)
    }


def query_kg4dataset(auth_token, dataset_id):
    """Query KG for metadata on a given dataset

    Parameters
    ----------
    token: str
      EBRAINS Auth-token
    dataset_id: str
      UUID of the target dataset

    Returns
    -------

    """
    # parameterize query with dataset ID
    url = "{}/instances?databaseScope=RELEASED&datasetId={}".format(
        kg_dataset_query,
        dataset_id,
    )
    # perform the request
    r = requests.get(
        url,
        headers=get_auth_headers(auth_token),
    )
    if not r.ok:
        raise KGQueryException(
            "request at {} failed ({}){}{}".format(
                r.url,
                r.status_code,
                ': ' if r.text else '',
                r.text))

    qres = json.loads(r.content)
    qres = validate_query_results(qres, dataset_id)
    return qres


def get_token(allow_interactive=True):
    # prefer the environment
    if 'EBRAINS_TOKEN' in environ:
        return environ['EBRAINS_TOKEN']

    # fall back on DataLad credential manager
    token_auth = Token(
        name='https://kg.humanbrainproject.eu/query',
        url='https://nexus-iam.humanbrainproject.org/v0/oauth2/authorize',
    )
    do_interactive = allow_interactive and ui.is_interactive()

    # get auth token, from environment, or from datalad credential store
    # if known-- we do not support first-time entry during a test run
    token = environ.get(
        'EBRAINS_TOKEN',
        token_auth().get('token', None)
        if do_interactive or token_auth.is_known
        else None)

    return token


class KGQueryException(RuntimeError):
    pass


def validate_query_results(res, dataset_id):
    """Perform basic validation of query result structure

    Parameters
    ----------
    res : dict
      Decoded JSON of query result
    dataset_id: str
      UUID of the target dataset

    Returns
    -------
    list
      Content of the first and only item in the `result` property
      list of the query output.
    """
    r = res.get('results', [])
    if not len(r):
        raise KGQueryException("yielded no result records")

    if len(r) > 1:
        raise KGQueryException("yielded more than one record")

    if res.get('start', None) != 0 \
            or res.get('size', 0)  != 1 \
            or res.get('total', 0) != 1:
        raise KGQueryException("results have unexpected/unsupported structure")

    # there is only a single record, simplify
    ds_res = r[0]

    if ds_res.get(ds_id_key) != dataset_id:
        raise KGQueryException(
            "results mismatch requested dataset ID")
    return ds_res


def get_kgds_parent_id(kgds):
    """Return ID of parent revision of the given dataset record

    Returns
    -------
    str or None
      If no revision ID is found, None is returned
    """
    revof = kgds.get(ds_revision_key, [])
    if not revof:
        return None
    if len(revof) > 1:
        lgr.warn(
            "More than on 'wasRevisionOf' for dataset record, "
            "proceeding with first entry "
            "(dataset will have incomplete version history)")
    return revof[0].get('https://schema.hbp.eu/myQuery/identifier', None)


def get_annex_key_records(kgds):
    """Map the KG query to a dict-per-file with properties for addurls

    Yields
    ------
    dict
    """
    for rec in kgds.get(ds_filelist_key, []):
        if not rec.get(type_key, None) == 'https://schema.hbp.eu/cscs/File':
            lgr.warn('Found non-file-type record, ignored')
        yield dict(
            url=rec['https://schema.hbp.eu/myQuery/absolute_path'],
            name=rec['https://schema.hbp.eu/myQuery/name'],
            md5sum=rec['https://schema.hbp.eu/myQuery/hash'],
            size=int(rec['https://schema.hbp.eu/myQuery/byte_size']),
            content_type=rec['https://schema.hbp.eu/myQuery/content_type'],
            last_modifier=rec[
                'https://schema.hbp.eu/myQuery/lastModificationUserId'],
            last_modified=rec['https://schema.hbp.eu/myQuery/last_modified'],
        )
