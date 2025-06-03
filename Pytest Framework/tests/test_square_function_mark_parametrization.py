import pytest

@pytest.mark.parametrize("input, expected", [
    (2, 4),
    (3, 9),
    (4, 16)
])
def test_square_function(input, expected):
    assert input ** 2 == expected