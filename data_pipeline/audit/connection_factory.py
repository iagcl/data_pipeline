###############################################################################
# Module:  connection_factory
# Purpose: Builds connection objects to the audit database
#
# Notes:
#
###############################################################################

import logging
import data_pipeline.constants.const as const

from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine


logger = logging.getLogger(__name__)
engine = None


def build_engine(conn_details):
    port_str = const.EMPTY_STRING
    dbsid_str = const.EMPTY_STRING

    if conn_details.port:
        port_str = ":{}".format(conn_details.port)

    if conn_details.dbsid:
        dbsid_str = "/{}".format(conn_details.dbsid)

    connection_string = 'postgresql://{}:{}@{}{}{}'.format(
        conn_details.userid,
        conn_details.password,
        conn_details.host,
        port_str,
        dbsid_str
    )

    logger.debug("ProcessControl: Connecting to '{}'"
                 .format(connection_string))

    engine = create_engine(connection_string,
                           connect_args={
                               'connect_timeout': conn_details.connect_timeout
                           },
                           pool_recycle=3600)

    logger.debug("ProcessControl: Successfully connected")

    return engine


def build_session(conn_details):
    global engine

    if engine is None:
        logger.debug("Building new audit connection engine")
        engine = build_engine(conn_details)

    Session = sessionmaker()
    Session.configure(bind=engine)
    session = Session()
    return session
