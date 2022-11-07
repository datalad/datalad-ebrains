import logging
from typing import (
    Dict,
    List,
    Tuple,
)

from datalad_next.patches import clone_utils as origmod

from datalad.runner.exception import CommandError
from datalad.distribution.dataset import Dataset

lgr = logging.getLogger('datalad.core.distributed.clone')


def _try_clone_candidate(
        *,
        destds: Dataset,
        cand: Dict,
        clone_opts: List) -> Tuple:
    """Attempt a clone from a single candidate

    destds: Dataset
      The target dataset the clone should materialize at.
    candidate_sources: list
      Each value is a dict with properties, as returned by
      `_generate_candidate_clone_sources()`
    clone_opts: list
      Options to be passed on to `_try_clone_candidate()`

    Returns
    -------
    (str, str or None, dict or None)
      The first item is the effective URL a clone was attempted from.
      The second item is `None` if the clone was successful, or an
      error message (specifically a CommandError), detailing the failure
      for the specific URL.
      If the third item is not `None`, it must be a result dict that
      should be yielded, and no further clone attempt (even when
      other candidates remain) will be attempted.
    """
    if not cand.get('source', '').startswith(
            'https://search.kg.ebrains.eu/instances/'):
        return orig_try_clone_candidate(
            destds=destds,
            cand=cand,
            clone_opts=clone_opts,
        )

    # this is a potential dataset instance landing page
    from datalad_ebrains.kg_query import (
        get_token,
        query_kg4dataset,
        KGQueryException,
    )
    from datalad_ebrains.kg2ds import process_revision

    source_url = cand['source']
    # pull the dataset ID from the URL
    # we are not checking whether this is formatted like a UUID, because we
    # know about some cases, whether it is not and is nevertheless correct
    # from the KG point of view
    kgdsid = source_url.split('/')[4]

    if not kgdsid:
        raise ValueError('No EBRAINS dataset ID given in clone URL')

    auth_token = get_token()

    try:
        query_res = query_kg4dataset(auth_token, kgdsid)
    except KGQueryException as e:
        return (
            source_url,
            # shoehorn into CommandError due to
            # https://github.com/datalad/datalad/issues/7148
            CommandError(stderr=e),
            None,
        )

    # if we got here, the basic query went well. time to create a dataset
    destds.create(result_renderer='disabled')

    # we need to unwind the generator here, clone() in its present shape does
    # not support yielding interim results
    # TODO best to wrap this into a progress bar
    try:
        list(process_revision(destds, kgdsid, query_res, auth_token))
    except Exception as e:
        # Cannot be msg= due to https://github.com/datalad/datalad/issues/7148
        return source_url, CommandError(stderr=str(e)), None

    # success
    return source_url, None, None


# apply patch
lgr.debug(
    'Apply datalad-ebrains patch to clone_utils.py:_try_clone_candidate()')
orig_try_clone_candidate = origmod._try_clone_candidate
origmod._try_clone_candidate = _try_clone_candidate
