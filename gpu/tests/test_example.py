import example_py


def test_add():
    assert example_py.add(1, 2) == 3
    assert example_py.add(-1, 1) == 0
    assert example_py.add(-1, -1) == -2
