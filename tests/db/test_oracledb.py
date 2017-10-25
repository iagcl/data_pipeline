import cx_Oracle
import pytest

import data_pipeline.constants.const as const

from pytest_mock import mocker
from data_pipeline.db.oracledb import OracleDb
from data_pipeline.db.connection_details import ConnectionDetails


@pytest.fixture
def setup(mocker):
    db = OracleDb()

    connection_details = ConnectionDetails(
        userid="myuserid", password="mypassword",
        host="myhost", port=1234, dbsid="mydbsid")

    mock_cursor_config = {
        "execute.return_value": None,
        "rowcount": 99,
        "description": ['a', 'b']
    }
    mock_cursor = mocker.Mock(**mock_cursor_config)

    def connect_se(connection_details):
        connected = True

    def close_se():
        connected = False

    mock_connection_config = {
        "connect.side_effect": connect_se,
        "close.side_effect": close_se,
        "cursor.return_value": mock_cursor,
        "closed": 0,
    }

    mock_connection = mocker.Mock(**mock_connection_config)

    def makedsn_se(host, port, dbsid):
        return cx_Oracle.makedsn(host, port, dbsid)

    mock_cx_oracle = mocker.patch("data_pipeline.db.oracledb.cx_Oracle")
    mock_cx_oracle.makedsn.side_effect = makedsn_se
    mock_cx_oracle.connect.return_value = mock_connection

    yield(db, connection_details, mock_cx_oracle, mock_connection, mock_cursor)


def test_execute_stored_proc(mocker, setup):
    (db, connection_details, mock_cx_oracle, mock_connection, mock_cursor) = setup
    db.connect(connection_details)
    mock_stored_proc = "this is a stored proc"
    db.execute_stored_proc(mock_stored_proc)
    mock_cx_oracle.connect.assert_called_with(user="myuserid", password="mypassword", dsn="(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=myhost)(PORT=1234))(CONNECT_DATA=(SID=mydbsid)))")
    mock_cursor.execute.assert_called_with("begin {}; end;".format(mock_stored_proc))


def test_execute_query(mocker, setup):
    (db, connection_details, mock_cx_oracle, mock_connection, mock_cursor) = setup
    db.connect(connection_details)
    mock_query = "this is a query"
    mock_arraysize = 10
    db.execute_query(mock_query, mock_arraysize)
    mock_cx_oracle.connect.assert_called_with(user="myuserid", password="mypassword", dsn="(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=myhost)(PORT=1234))(CONNECT_DATA=(SID=mydbsid)))")
    mock_cursor.execute.assert_called_with(mock_query)


def test_execute(mocker, setup):
    (db, connection_details, mock_cx_oracle, mock_connection, mock_cursor) = setup
    db.connect(connection_details)
    mock_sql = "this is some sql"
    mock_bindings = ('a', 'b')
    count = db.execute(mock_sql, mock_bindings)
    mock_cx_oracle.connect.assert_called_with(user="myuserid", password="mypassword", dsn="(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=myhost)(PORT=1234))(CONNECT_DATA=(SID=mydbsid)))")
    mock_cursor.execute.assert_called_with(mock_sql, mock_bindings)
    assert count == 99


def test_commit(mocker, setup):
    (db, connection_details, mock_cx_oracle, mock_connection, mock_cursor) = setup
    db.connect(connection_details)
    db.commit()
    mock_cx_oracle.connect.assert_called_with(user="myuserid", password="mypassword", dsn="(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=myhost)(PORT=1234))(CONNECT_DATA=(SID=mydbsid)))")
    mock_connection.commit.assert_called_once()


def test_rollback(mocker, setup):
    (db, connection_details, mock_cx_oracle, mock_connection, mock_cursor) = setup
    db.connect(connection_details)
    db.rollback()
    mock_cx_oracle.connect.assert_called_with(user="myuserid", password="mypassword", dsn="(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=myhost)(PORT=1234))(CONNECT_DATA=(SID=mydbsid)))")
    mock_connection.rollback.assert_called_once()


def test_disconnect(mocker, setup):
    (db, connection_details, mock_cx_oracle, mock_connection, mock_cursor) = setup
    db.connect(connection_details)
    db.disconnect()
    mock_cx_oracle.connect.assert_called_with(user="myuserid", password="mypassword", dsn="(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=myhost)(PORT=1234))(CONNECT_DATA=(SID=mydbsid)))")
    mock_cursor.close.assert_called_once()
    mock_connection.close.assert_called_once()


def test_close(mocker, setup):
    (db, connection_details, mock_cx_oracle, mock_connection, mock_cursor) = setup
    db.connect(connection_details)
    db.close()
    mock_cx_oracle.connect.assert_called_with(user="myuserid", password="mypassword", dsn="(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=myhost)(PORT=1234))(CONNECT_DATA=(SID=mydbsid)))")
    mock_cursor.close.assert_called_once()
    mock_connection.close.assert_called_once()

def test_closed(mocker, setup):
    (db, connection_details, mock_cx_oracle, mock_connection, mock_cursor) = setup
    db.connect(connection_details)
    assert not db.closed()
    mock_cx_oracle.connect.assert_called_with(user="myuserid", password="mypassword", dsn="(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=myhost)(PORT=1234))(CONNECT_DATA=(SID=mydbsid)))")
