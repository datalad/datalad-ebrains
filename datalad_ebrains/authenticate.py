import sys

from datalad import cfg as dlcfg
from datalad_next.commands import (
    Interface,
    build_doc,
    generic_result_renderer,
    get_status_dict,
    eval_results,
)
from datalad_next.exceptions import CapturedException
from datalad_next.uis import ui_switcher as ui


@build_doc
class Authenticate(Interface):
    """Obtain an EBRAINS authentication token

    This command executes the "device authorization flow". It will present a
    URL to a webpage. Please note that any period ('.') at the end of the
    displayed URL is not part of the URL.

    On the linked page, a user can log in with their EBRAINS credentials. When
    successfully logged in, this command will return a token that can be used
    to authenticate for additional operations.

    A common usage in a POSIX shell would be::

        export KG_AUTH_TOKEN=`datalad ebrains-authenticate`

    which assigns the token to the ``KG_AUTH_TOKEN`` environment variable.
    This variable is, for example, honored by the ``ebrains-clone`` command.
    """
    @staticmethod
    @eval_results
    def __call__():
        from kg_core.kg import kg
        k = kg()

        oidc_secret = dlcfg.get('datalad.tests.ebrains-oidc-secret')
        if oidc_secret:
            # we know an OIDC client secret to use with the CI client
            # this will only happen in specifcally set up test environments
            k.with_credentials(
                'datalad-ebrains-ci',
                oidc_secret,
            ).build()
        else:
            # this will present instructions, which URL to visit, and will
            # return with a token str, once a user has successfully
            # authenticated at EBRAINS
            k.with_device_flow()
        try:
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
