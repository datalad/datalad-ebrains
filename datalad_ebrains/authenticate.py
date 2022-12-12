import sys

from datalad.interface.base import (
    Interface,
    build_doc,
)
from datalad.interface.results import get_status_dict
from datalad.interface.utils import (
    generic_result_renderer,
    eval_results,
)
from datalad.support.exceptions import CapturedException
from datalad.ui import ui


@build_doc
class Authenticate(Interface):
    """Obtain an EBRAINS authentication token

    This command executes the "device authorization flow". It will present a
    URL to a webpage. On this page, a user can log in with their EBRAINS
    credentials. When successfully logged in, this command will return
    a token that can be used to authenticate for additional operations.

    A common usage in a POSIX shell would be::

        export KG_AUTH_TOKEN=`datalad ebrains-authenticate`

    which assignes the token to the ``KG_AUTH_TOKEN`` environment variable.
    This variable is, for example, honored by the ``ebrains-clone`` command.
    """
    @staticmethod
    @eval_results
    def __call__():
        from kg_core.kg import kg
        k = kg()

        # this will present instructions, which URL to visit, and will return
        # with a token str, once a user has successfully auhtenticated at
        # EBRAINS
        try:
            k.with_device_flow()
            # we redirect stdout to stderr temporarily to be able to only
            # output the token on stdout, separate from the instructions that
            # kg-core unconditionally prints to stdout.  this facilitates
            # external reuse of the token (e.g. define as an ENV var)
            with _RedirectStdErr2StdOut():
                token = k._token_handler.get_token()
        except Exception as e:
            yield get_status_dict(
                action='ebrains-authenticate',
                status='error',
                exception=CapturedException(e),
            )
            return
        yield get_status_dict(
            action='ebrains-authenticate',
            status='ok',
            token=token,
        )

    @staticmethod
    def custom_result_renderer(res, **kwargs):
        if res['status'] != 'ok' or res['action'] != 'ebrains-authenticate' \
                or 'token' not in res:
            # not what we were aiming -> report
            generic_result_renderer(res)
            return
        # naked print of the token, rather then some precrafted KG_TOKEN=...
        # line. This should offer maximum flexibility for reuse on different
        # platforms/shells
        ui.message(res["token"])


class _RedirectStdErr2StdOut:
    """Utility context manager to redirect stdout to stderr"""
    def __enter__(self):
        self._stdout = sys.stdout
        sys.stdout = sys.stderr
        return self

    def __exit__(self, *args):
        sys.stdout = self._stdout
