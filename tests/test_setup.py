import pytest
from pyeztrace import setup, exceptions

@pytest.fixture(autouse=True)
def reset_setup():
    setup.Setup.reset()

def test_initialize_and_getters():
    setup.Setup.initialize("EZTRACER_TEST", show_metrics=True)
    assert setup.Setup.is_setup_done()
    assert setup.Setup.get_project() == "EZTRACER_TEST"
    assert setup.Setup.get_level() == 0
    assert setup.Setup.get_show_metrics() is True

def test_increment_decrement_level():
    setup.Setup.initialize("EZTRACER_TEST2", show_metrics=False)
    setup.Setup.increment_level()
    assert setup.Setup.get_level() == 1
    setup.Setup.decrement_level()
    assert setup.Setup.get_level() == 0

def test_double_initialize_raises():
    setup.Setup.initialize("EZTRACER_TEST3", show_metrics=False)
    with pytest.raises(exceptions.SetupAlreadyDoneError):
        setup.Setup.initialize("EZTRACER_TEST3", show_metrics=False)