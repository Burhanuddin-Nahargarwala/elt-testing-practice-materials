import sys
import os

# Add the src/ directory to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'src')))

from my_module import add


def test_add():
    assert add(2, 3) == 5
    assert add(-1, 1) == 0

    # error
    assert add('2', '3') == 5
