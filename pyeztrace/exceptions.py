class SetupNotDoneError(Exception):
    """
    Exception raised when setup is not done.
    """
    pass

class SetupAlreadyDoneError(Exception):
    """
    Exception raised when setup is already done.
    """
    pass