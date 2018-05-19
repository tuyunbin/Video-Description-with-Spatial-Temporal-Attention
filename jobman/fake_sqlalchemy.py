"""Used to fake some component of sqlalchemy to avoid errors at jobman import.

This is used only when sqlalchemy is not installed, and does not provide any
actual working sqlalchemy-like code.
"""

class pool:
    NullPool = None

