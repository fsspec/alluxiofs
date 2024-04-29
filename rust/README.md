## A PyO3 based common native extension lib for alluxio python client

### Developer Prerequisites:
- Install Rust:
https://www.rust-lang.org/tools/install
- Install maturin:
https://www.maturin.rs/installation


### To build developer version locally and play:

1) create virtualenv, (a tool used to create isolated Python environments):


    python3 -m venv .env
    source .env/bin/activate
    maturin develop

2) then can start using:


    python3
    >>> from alluxiocommon import _DataManager
    >>> dm = _DataManager(4)
    >>> # do something with dm...

### To build wheel package and install with pip:

    #in rust/alluxiocommon dir:
    $ maturin build --out <alluxiofs_home>/dist -m <alluxiofs_home>/rust/alluxiocommon/Cargo.toml -i python<x.y> (python version such as 3.8)
    #then find .whl package in <alluxiofs_home>/dist:
    [root@ip-XXX-XX-XX-XX alluxiofs]# ls -l dist/
    total 21848
    -rw-r--r--. 1 root root 22318133 Apr 28 05:31 alluxiocommon-0.1.0-cp38-cp38-linux_x86_64.whl
    #install with pip
    $ pip install dist/alluxiocommon-0.1.0-cp38-cp38-linux_x86_64.whl --force-reinstall
