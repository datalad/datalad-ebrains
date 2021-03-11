from datalad.tests.utils import assert_result_count


def test_register():
    import datalad.api as da
    assert hasattr(da, 'ebrains_kg2ds')
    # Bring back later with actual usage, once we can do that
    #assert_result_count(
    #    da.ebrains_kg2ds(),
    #    1,
    #    action='demo')

