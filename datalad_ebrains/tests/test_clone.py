import pytest

from datalad_next.tests.utils import (
    assert_in_results,
    assert_result_count,
)


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


def test_shallow_clone(tmp_path):
    from datalad.api import (
        Dataset,
        ebrains_clone,
    )
    # clone Jülich Brain Atlas v3.0 and only this version
    res = ebrains_clone(
        'https://search.kg.ebrains.eu/instances/900a1c2d-4914-42d5-a316-5472afca0d90',
        tmp_path,
        depth=1,
        result_renderer='disabled',
    )
    repo = Dataset(tmp_path).repo
    if not repo.is_managed_branch():
        # not ready for crippled FS
        # https://github.com/datalad/datalad-ebrains/issues/55
        check_branch = repo.get_corresponding_branch() \
            or repo.get_active_branch()
        log_cmd = ['log', '--oneline', check_branch]
        # the desired version, plus the create-dataset-commit
        assert 2 == len(list(repo.call_git_items_(log_cmd)))
    # EBRAIN landing page states: 1678 files -- we must match that number
    assert_result_count(res, 1678, type='file')


def test_ebrains_clone(tmp_path):
    from datalad.api import ebrains_clone
    # smoke test, leave result rendering on to see what happens in the logs
    ebrains_clone(
        # this will pull many version, and use different file repository types
        # across these versions
        'https://search.kg.ebrains.eu/instances/5a16d948-8d1c-400c-b797-8a7ad29944b2',
        tmp_path)


def test_clone_invalid_call(tmp_path):
    # make sure the parameter validation is working
    from datalad.api import ebrains_clone
    # always needs a `source`
    with pytest.raises(TypeError):
        ebrains_clone()
    # must contain a UUID
    with pytest.raises(ValueError):
        ebrains_clone('bogus')
    # depth must be an int
    with pytest.raises(ValueError):
        ebrains_clone('5a16d948-8d1c-400c-b797-8a7ad29944b2', depth='mike')
    # depth must be larger than 0
    with pytest.raises(ValueError):
        ebrains_clone('5a16d948-8d1c-400c-b797-8a7ad29944b2', depth=0)
