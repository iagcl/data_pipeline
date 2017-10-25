from version import get_version
from pytest_mock import mocker


def test_get_version(mocker):
    mock_check_output = mocker.patch("version.check_output")
    mock_check_output.return_value = "v0.4.2-32-ga2ccd08"
    version = get_version()
    assert version == "0.4.2.post32"


def test_get_version(mocker):
    mock_check_output = mocker.patch("version.check_output")
    mock_check_output.return_value = "v0.4.2-32-ga2ccd08-dirty"
    version = get_version()
    assert version == "0.4.2.post32*"
