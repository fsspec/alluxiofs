def get_md5(*contents):
    from hashlib import md5

    m = md5()
    for content in contents:
        m.update(content)
    return m.hexdigest()


def test_import_alluxiocommon():
    try:
        from alluxiocommon import _DataManager

        dm = _DataManager(4)
        print(f"alluxiocommon._DataManager:{dm} instantiated!")
        urls = ["http://example.com", "http://example.net"]
        concatenated_body = dm.make_multi_http_req(urls)
        md5_outcome = get_md5(concatenated_body)
        import requests

        md5_expected = get_md5(
            requests.get(urls[0]).content, requests.get(urls[1]).content
        )
        assert (
            md5_outcome == md5_expected
        ), f"expected md5:{md5_expected} got:{md5_outcome} instead,"
        f"len(concatenated_body):{len(concatenated_body)}"
    except Exception as e:
        assert (
            False
        ), f"Unexpected exception when importing alluxiocommon package:{e}"
