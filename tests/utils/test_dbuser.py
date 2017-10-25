import pytest
from data_pipeline.utils.dbuser import get_dbuser_properties


def test_get_dbuser_properties_empty():
    conn_details = get_dbuser_properties('')
    assert conn_details is None


@pytest.mark.parametrize("input_dbuser, input_password, keyring_password, expect_getpass_called_times, expect_userid, expect_password, expect_host, expect_port, expect_dbsid, expect_error", [
    ('someone/password@host:1234/dbsid', None, None, 0, "someone", "password", "host", "1234", "dbsid", None),
    ('someone/password@host/dbsid', None, None, 0, "someone", "password", "host", None, "dbsid", None),
    ('someone/password@host:1234', None, None, 0, "someone", "password", "host", "1234", None, None),
    ('someone/p@ssword@host:1234', None, None, 0, "someone", "p@ssword", "host", "1234", None, None),
    ('someone/p@sswor@@host:1234', None, None, 0, "someone", "p@sswor@", "host", "1234", None, None),
    ('s@meone/p@sswor@@host:1234', None, None, 0, "s@meone", "p@sswor@", "host", "1234", None, None),
    ('someone/password@host', None, None, 0, "someone", "password", "host", None, None, None),
    ('someone@host:1234/dbsid', "userinputpassword", None, 2, "someone", "userinputpassword", "host", "1234", "dbsid", None),
    ('someone@host:1234/dbsid', None, "userinputpassword", 0, "someone", "userinputpassword", "host", "1234", "dbsid", None),
    ('someone@:1234/dbsid', None, None, 0, "someone", None, None, "1234", "dbsid", "Invalid connection string: Host missing."),
    ('@host:1234/dbsid', None, None, 0, None, None, "host", "1234", "dbsid", "Invalid connection string: User ID missing."),
])
def test_get_dbuser_properties(mocker, input_dbuser, input_password, keyring_password, expect_getpass_called_times, expect_userid, expect_password, expect_host, expect_port, expect_dbsid, expect_error):

    get_password_func = mocker.patch("data_pipeline.utils.dbuser.keyring.get_password")
    get_password_func.return_value = keyring_password

    getpass_func = mocker.patch("data_pipeline.utils.dbuser.getpass.getpass")
    getpass_func.return_value = input_password

    set_password_func = mocker.patch("data_pipeline.utils.dbuser.keyring.set_password")

    if expect_error:
        with pytest.raises(ValueError) as e:
            conn_details = get_dbuser_properties(input_dbuser)
            print(conn_details)
        assert str(e.value).startswith(expect_error)
    else:
        conn_details = get_dbuser_properties(input_dbuser)
        assert conn_details.userid == expect_userid
        assert conn_details.password == expect_password
        assert conn_details.host == expect_host
        assert conn_details.port == expect_port
        assert conn_details.dbsid == expect_dbsid
        assert getpass_func.call_count == expect_getpass_called_times

@pytest.mark.parametrize("input_data_dir, input_datafile, expect_error", [
    ("data", "tableA.csv", None),
    ("data", None, "Directory is empty"),
    (None, None, "No such directory"),
])
def test_get_dbuser_properties_data_dir(
        input_data_dir, input_datafile, expect_error, tmpdir):

    data_dir = None
    if input_data_dir:
        data_dir = tmpdir.mkdir(input_data_dir)

    if input_datafile and data_dir:
        filename = str(data_dir.join(input_datafile))
        open(filename, 'a').close()

    data_dir = str(data_dir)
    if expect_error:
        with pytest.raises(ValueError) as e:
            conn_details = get_dbuser_properties(data_dir)
        assert str(e.value).startswith(expect_error)
    else:
        conn_details = get_dbuser_properties(data_dir)
        assert conn_details.data_dir == data_dir
