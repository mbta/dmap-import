from cubic_loader import dmap


def test_version() -> None:
    """
    assert that we can import the cubic_loader library and access its version
    """
    assert dmap.__version__ == "0.1.0"
