import json
import logging
import os
from pathlib import Path
import requests
from urllib.parse import quote as urlquote


lgr = logging.getLogger('datalad.ebrains.kg_query')

# use query endpoint of a stored query for datasets
# see tools/kg_query.py
# TODO these are beta endpoints, finalize!
kg_dataset_query = "https://core.kg.ebrains.eu/v3-beta/queries"
kg_file_query = "https://core.kg.ebrains.eu/v3-beta/queries"

ds_id_key = 'id'
ds_revision_key = 'https://schema.hbp.eu/myQuery/wasRevisionOf'

# TODO: define all of them
accessibility_modes = {
    'free_access':
    'https://openminds.ebrains.eu/instances/productAccessibility/freeAccess',
    'under_embargo':
    'https://openminds.ebrains.eu/instances/productAccessibility/underEmbargo',
}


def get_auth_headers(token):
    """Get token-based auth headers for a KG HTTP request

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
        'accept': '*/*',
        'Content-Type': 'application/json',
        'Authorization': 'Bearer {}'.format(token)
    }


def _query_kg(auth_token, url, query_fname):
    lgr.debug('POST request for %s', url)
    # perform the request
    # it actually posts the full request body
    dsquery_file = Path(__file__).parent / 'resources' / query_fname
    dsquery = dsquery_file.read_text()
    r = requests.post(
        url,
        headers=get_auth_headers(auth_token),
        data=dsquery,
    )
    if not r.ok:
        raise KGQueryException(
            "request at {} failed ({}){}{}".format(
                r.url,
                r.status_code,
                ': ' if r.text else '',
                r.text))

    qres = json.loads(r.content)
    return qres


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
    url = "{}?stage=RELEASED&instanceId={}".format(
        kg_dataset_query,
        dataset_id,
    )
    qres = _query_kg(auth_token, url, 'dataset_query_v3beta.json')
    qres = validate_query_results(qres, dataset_id)
    return qres


def get_token(credential, allow_interactive=True):
    if 'DATALAD_ebrains_token' in os.environ:
        return os.environ['DATALAD_ebrains_token']
    elif 'KG_TOKEN' in os.environ:
        return os.environ['KG_TOKEN']
    else:
        raise RuntimeError(
            'No EBRAINS access token defined in environment, '
            'see datalad ebrains-authenticate command for more information.')


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
    r = res.get('data', [])
    if not len(r):
        raise KGQueryException("yielded no result records")

    if len(r) > 1:
        raise KGQueryException("yielded more than one record")

    if res.get('from', None) != 0 \
            or res.get('size', 0)  != 1 \
            or res.get('total', 0) != 1:
        raise KGQueryException("results have unexpected/unsupported structure")

    query_duration = res.get('durationInMs')
    if query_duration:
        lgr.debug('Knowledge graph query duration: %sms', query_duration)

    # there is only a single record, simplify
    ds_res = r[0]

    if not ds_res.get(ds_id_key).endswith(dataset_id):
        raise KGQueryException(
            "results mismatch requested dataset ID")
    return ds_res


def query_kg4files(auth_token, filerepo_id):
    url = "{}?stage=IN_PROGRESS&fileRepositoryId={}".format(
        kg_file_query,
        urlquote(filerepo_id, safe=''),
    )
    qres = _query_kg(auth_token, url, 'file_query_v3beta.json')
    return qres


def get_annex_key_records(
        repo_id,
        repo_baseurl,
        auth_token,
):
    """Map the KG query to a dict-per-file with properties for addurls

    Yields
    ------
    dict
    """
    # TODO fold query_kg4files() into get_annex_key_records()
    # to be able to perform as many lean query as necessary,
    # and yield recs one by one, there could be a huge number of
    # file records
    file_query = query_kg4files(auth_token, repo_id)

    for rec in file_query.get('data', []):
        annexrec = _filerec2annexrec(rec, repo_baseurl)
        if annexrec:
            yield annexrec


def _filerec2annexrec(rec, baseurl):
    iri = rec['iri']
    if not iri.startswith(baseurl):
        lgr.warning('File IRI does not match file repo IRI, ignored')
        return
    # this is different from rec['name'], because it includes the
    # filerepo 'prefix' and any subdirectories. Both are important to
    # get the internal dataset organization right

    props = dict(
        url=iri,
        # make sure to strip any leading '/' to ensure a relative path
        # TODO double-check windows FS semantics
        name=iri[len(baseurl):].lstrip('/'),
    )
    md5sum = None
    for hsh in rec.get('hash', {}):
        if hsh.get('algorithm') == 'MD5':
            md5sum = hsh.get('digest')
    if md5sum:
        props['md5sum'] = md5sum

    size = rec.get('size', {}).get('value') \
        if rec.get('size', {}).get('unit') == 'byte' else None
    if size is not None:
        props['size'] = size

    content_type = rec.get('format', {}).get('fullName')
    if content_type:
        props['content_type'] = content_type

    # TODO can we get a modification time?
    # can we get the entity that last modified this file record
    #last_modifier=rec[
    #    'https://schema.hbp.eu/myQuery/lastModificationUserId'],
    #last_modified=rec['https://schema.hbp.eu/myQuery/last_modified'],

    return props
