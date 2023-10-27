import dmap_import


def test_version() -> None:
    """
    assert that we can import the dmap_import library and access its version
    """
    assert dmap_import.__version__ == "0.1.0"
