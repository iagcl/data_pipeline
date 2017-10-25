###############################################################################
# Module:    process_control_dao
# Purpose:   Defines the Data Access Objects for the family of Process Control
#            tables
#
# Notes:
#
###############################################################################

import datetime
import logging
import data_pipeline.constants.const as const

from data_pipeline.utils.utils import dump
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base, declared_attr
from sqlalchemy import (Column, ForeignKey, Integer, String, DateTime,
                        Numeric, BigInteger, Text, Sequence)


Base = declarative_base()


class ProcessControlBase(object):
    def __init__(self, session, argv):
        self._session = session
        self._logger = logging.getLogger(__name__)
        self._argv = argv

    def _truncate_comment(self):
        self.comment = self.comment[:const.MAX_COMMENT_LENGTH]

    def insert(self):
        if self._session:
            try:
                self._truncate_comment()
                self._session.add(self)
                self._session.flush()
                self._session.refresh(self)
                self._session.commit()
                if self._argv.veryverbose:
                    self._logger.debug("insert(): {}".format(str(self)))
            except Exception, e:
                self._session.rollback()
                self._logger.exception("Insert failed. Transaction rolled "
                                       "back. {}".format(str(e)))

    def update(self):
        if self._session:
            try:
                self._truncate_comment()
                self.process_endtime = datetime.datetime.now()
                timediff = self.process_endtime - self.process_starttime
                self.duration = timediff.total_seconds()
                self._session.commit()
                if self._argv.veryverbose:
                    self._logger.debug("update(): {}".format(self))
            except Exception, e:
                self._session.rollback()
                self._logger.exception("Update failed. Transaction rolled "
                                       "back. {}".format(str(e)))

    def delete(self):
        if self._session:
            try:
                self._session.delete(self)
                self._session.commit()
            except Exception, e:
                self._session.rollback()
                self._logger.exception("Delete failed. Transaction rolled "
                                       "back. {}".format(str(e)))


PCBase = declarative_base(cls=ProcessControlBase,
                          constructor=ProcessControlBase.__init__)


class ProcessControl(PCBase):
    __tablename__ = 'process_control'

    id = Column(Integer, Sequence('process_control_id_seq'), primary_key=True)
    details = relationship("ProcessControlDetail", backref="process_control")
    profile_name = Column(String(20))
    profile_version = Column(Integer)
    process_code = Column(String(20))
    process_name = Column(String(30))
    source_system_code = Column(String(10))
    source_system_type = Column(String(10))
    source_region = Column(String(30))
    target_system = Column(String(30))
    target_system_type = Column(String(10))
    target_region = Column(String(30))
    process_starttime = Column(DateTime)
    process_endtime = Column(DateTime)
    min_lsn = Column(String(30))
    max_lsn = Column(String(30))
    status = Column(String(20))
    duration = Column(Numeric(precision=20, scale=4))
    comment = Column(String(2048))
    filename = Column(String(1024))
    executor_run_id = Column(BigInteger)
    executor_status = Column(String(10))
    object_list = Column(Text)
    infolog = Column(String(1024))
    errorlog = Column(String(1024))
    total_count = Column(BigInteger)

    def __str__(self):
        return dump(self)


class ProcessControlDetail(PCBase):
    __tablename__ = 'process_control_detail'

    id = Column(Integer, Sequence('process_control_detail_id_seq'),
                primary_key=True)
    run_id = Column(Integer, ForeignKey('process_control.id'))
    process_code = Column(String(20))
    object_schema = Column(String(30))
    object_name = Column(String(50))
    process_starttime = Column(DateTime)
    process_endtime = Column(DateTime)
    status = Column(String(20))
    source_row_count = Column(BigInteger)
    insert_row_count = Column(BigInteger)
    update_row_count = Column(BigInteger)
    delete_row_count = Column(BigInteger)
    bad_row_count = Column(BigInteger)
    alter_count = Column(BigInteger)
    create_count = Column(BigInteger)
    duration = Column(Numeric(precision=20, scale=4))
    delta_starttime = Column(DateTime)
    delta_endtime = Column(DateTime)
    delta_startlsn = Column(String(30))
    delta_endlsn = Column(String(30))
    error_message = Column(String(300))
    comment = Column(String(300))
    query_condition = Column(String(2048))
    infolog = Column(String(1024))
    errorlog = Column(String(1024))

    def __str__(self):
        return dump(self)


class SourceSystemProfile(Base):
    __tablename__ = 'source_system_profile'

    id = Column(Integer, Sequence('source_system_profile_id_seq'),
                primary_key=True)
    profile_name = Column(String(20))
    version = Column(Integer)
    source_system_code = Column(String(10))
    source_region = Column(String(30))
    target_region = Column(String(30))
    object_name = Column(String(50))
    object_seq = Column(BigInteger)
    min_lsn = Column(String(30))
    max_lsn = Column(String(30))
    active_ind = Column(String(1))
    history_ind = Column(String(1))
    applied_ind = Column(String(1))
    delta_ind = Column(String(1))
    last_run_id = Column(Integer)
    last_process_code = Column(String(20))
    last_status = Column(String(20))
    last_updated = Column(DateTime)
    last_applied = Column(DateTime)
    last_history_update = Column(DateTime)
    notes = Column(String(4000))
