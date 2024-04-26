def test_import_alluxiocommon():
    try:
        from alluxiocommon import _DataManager

        dm = _DataManager()
        print(f"alluxiocommon._DataManager:{dm} instantiated!")
    except Exception as e:
        assert (
            False
        ), f"Unexpected exception when importing alluxiocommon package:{e}"
