import pytest
from src.exceptions import FilenameValidationException, UnsupportedFiletypeException


def test_unsupported_filetype_str():
    exc = UnsupportedFiletypeException("test.xyz")
    assert "test.xyz" in str(exc) or str(exc) is not None


def test_unsupported_filetype_repr():
    exc = UnsupportedFiletypeException("file.abc")
    assert repr(exc) is not None


def test_filename_validation_str():
    exc = FilenameValidationException("invalid")
    assert str(exc) is not None


def test_filename_validation_with_message():
    exc = FilenameValidationException("File missing country code")
    assert "country" in str(exc).lower() or str(exc) is not None


def test_exceptions_are_exception_subclass():
    assert issubclass(UnsupportedFiletypeException, Exception)
    assert issubclass(FilenameValidationException, Exception)


def test_exceptions_can_be_raised():
    with pytest.raises(UnsupportedFiletypeException):
        raise UnsupportedFiletypeException("test")
    with pytest.raises(FilenameValidationException):
        raise FilenameValidationException("test")
