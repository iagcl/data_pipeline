###############################################################################
# Module:    filedb
# Purpose:   Contains file specific initsync functions
#
# Notes:
#
###############################################################################


from exceptions import NotSupportedException
from initsyncdb import InitSyncDb


class FileDb(InitSyncDb):
    def __init__(self, argv, db, logger):
        super(FileDb, self).__init__(argv, db, logger)

    def get_source_column_list(self, table):
        raise NotSupportedException(
            "get_source_column_list currently not supported for Files")

    def get_target_column_list(self, table):
        raise NotSupportedException(
            "get_target_column_list currently not supported for Files")

    def _pre_extract(self):
        pass

    def _post_extract(self, record):
        return record.split(self._argv.delimiter)

    def execute_post_processing(self, target_table):
        pass

    def _build_extract_data_sql(self, column_list, table, extractlsn,
                                samplerows, lock, query_condition):
        """For file-based extracts, the query string is simply the
        tablename and, by convention, the underlying FileDb will
        search for files with a basename of the tablename.
        For example, if tablename = 'foo', then FileDb match any of the
        following file names (and use the first matching one):
            - foo.bar
            - foo.bar.gz
            - foo.bar.bz2
            - foo.csv
        but will not match the following:
            - foobar.csv
            - foobar.csv.gz
            - foo-bar.csv
            - foo_bar.csv
        """
        return str(table.name)

    def table_exists(self, table):
        return self._db.get_data_filename(table.name) is not None

    def bulk_write(self, **kwargs):
        raise NotSupportedException(
            "bulk_write currently not supported for Files")

    def delete(self, table, query_condition):
        raise NotSupportedException(
            "delete currently not supported for Files")

    def truncate(self, table):
        raise NotSupportedException(
            "truncate currently not supported for Files")
