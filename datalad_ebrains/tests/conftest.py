import os
import pytest
from unittest.mock import patch


@pytest.fixture(scope="session", autouse=True)
def authenticate():
    if 'KG_AUTH_TOKEN' in os.environ:
        # we seem to have what we need
        yield
        return

    from datalad.api import ebrains_authenticate
    token = ebrains_authenticate(
        result_renderer='disabled',
        return_type='item-or-list',
    ).get('token')
    if not token:
        raise RuntimeError('Unable to obtain an access token')
    with patch.dict(
            'os.environ',
            {'KG_AUTH_TOKEN': token}):
        # all tests run in this adjusted environment
        yield
