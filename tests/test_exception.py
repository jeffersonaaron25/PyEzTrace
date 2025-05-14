import pytest
from pyeztrace.exceptions import SetupNotDoneError, SetupAlreadyDoneError

def test_exceptions_are_raised():
    with pytest.raises(SetupNotDoneError):
        raise SetupNotDoneError("not done")
    with pytest.raises(SetupAlreadyDoneError):
        raise SetupAlreadyDoneError("already done")