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
    v1_history = list(dsv1.repo.call_git_items_(['log', '--oneline']))
    v2_history = list(dsv2.repo.call_git_items_(['log', '--oneline']))
    # the two version histories must be bit-identical, for all shared versions
    assert v1_history == v2_history[1:]


def test_ebrain_clone(tmp_path):
    from datalad.api import ebrains_clone
    # smoke test, leave result rendering on to see what happens in the logs
    ebrains_clone(
        # this will pull 8 version, but is not pointing to a full dataset
        # (i.e. ALL versions), because of
        # https://github.com/datalad/datalad-ebrains/issues/47
        'https://kg.ebrains.eu/api/instances/a8932c7e-063c-4131-ab96-996d843998e9',
        tmp_path)
