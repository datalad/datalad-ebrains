from datalad_next.tests.utils import assert_in_results


def test_clone_reproducibility(tmp_path):
    clone_kwargs = dict(
        result_xfm='datasets',
        result_renderer='disabled',
    )
    from datalad.api import ebrains_clone
    # first version only
    dsv1 = ebrains_clone(
        'https://search.kg.ebrains.eu/instances/fd303d56-e1aa-46a2-9d0c-7e5215aeb7ca',
        tmp_path / 'v1',
        **clone_kwargs
    )[-1]
    # up to second version
    dsv2 = ebrains_clone(
        'https://search.kg.ebrains.eu/instances/4ac9f0bc-560d-47e0-8916-7b24da9bb0ce',
        tmp_path / 'v2',
        **clone_kwargs
    )[-1]
    # make sure we check the corresponding branch on crippled FSes
    # (will be the same for both datasets)
    check_branch = dsv1.repo.get_corresponding_branch() \
        or dsv1.repo.get_active_branch()
    log_cmd = ['log', '--oneline', check_branch]
    v1_history = list(dsv1.repo.call_git_items_(log_cmd))
    v2_history = list(dsv2.repo.call_git_items_(log_cmd))
    # the two version histories must be bit-identical, for all shared versions
    assert v1_history == v2_history[1:]


def test_unsupported_filerepo(tmp_path):
    from datalad.api import ebrains_clone
    res = ebrains_clone(
        'https://search.kg.ebrains.eu/instances/d07f9305-1e75-4548-a348-b155fb323d31',
        tmp_path,
        result_renderer='disabled',
        on_failure='ignore',
    )
    assert_in_results(
        res, status='impossible',
        error_message='Unrecognized file repository pointer '
        'https://ftp.bigbrainproject.org/bigbrain-ftp/BigBrainRelease.2015/',
    )


def test_ebrain_clone(tmp_path):
    from datalad.api import ebrains_clone
    # smoke test, leave result rendering on to see what happens in the logs
    ebrains_clone(
        # this will pull many version, and use different file repository types
        # across these versions
        'https://search.kg.ebrains.eu/instances/5a16d948-8d1c-400c-b797-8a7ad29944b2',
        tmp_path)
