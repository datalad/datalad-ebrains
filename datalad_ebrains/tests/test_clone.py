
def test_ebrain_clone(tmp_path):
    from datalad.api import ebrains_clone

    # smoke test
    ebrains_clone(
        'https://kg.ebrains.eu/api/instances/a8932c7e-063c-4131-ab96-996d843998e9',
        tmp_path)
