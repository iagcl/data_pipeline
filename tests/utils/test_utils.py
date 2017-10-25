import pytest
import data_pipeline.utils.utils as utils


FOO = 'foo'
BAR = 'bar'
CAR = 'car'


@pytest.mark.parametrize("attr, expected_a, expected_b, expected_c", [
    ({'c': CAR}, FOO, BAR, CAR),
    ({'a': CAR}, CAR, BAR, None),
    ({'b': CAR, 'c': FOO}, FOO, CAR, FOO),
])
def test_merge_attributes(attr, expected_a, expected_b, expected_c):
    class A:
        def __init__(self):
            self.a = FOO
            self.b = BAR

    a = A()
    assert a.a == FOO
    assert a.b == BAR

    c = utils.merge_attributes(a, attr)

    # Assert nothing's changed on the original object
    assert a.a == expected_a
    assert a.b == expected_b
    if expected_c is not None:
        assert a.c == expected_c
