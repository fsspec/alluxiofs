## A common native lib for alluxio python client
to build and play:

create virtualenv, (a tool used to create isolated Python environments): 

    python3 -m venv .env
    source .env/bin/activate
    maturin develop

then can do:

    python3
    >>> import alluxiocommon
    >>> alluxiocommon.multi_http_requests(["http://google.com"],[(0,0)])

