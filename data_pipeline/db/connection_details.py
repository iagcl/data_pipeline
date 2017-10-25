###############################################################################
# Module:    connection_details
# Purpose:   Contains all details necessary for establishing a database
#            connection
#
# Notes:
###############################################################################


class ConnectionDetails:
    def __init__(self, userid=None, password=None, host=None, port=None,
                 dbsid=None, connect_timeout=10, charset='utf8', as_dict=False,
                 sslmode=None, sslcert=None, sslrootcert=None, sslkey=None,
                 sslcrl=None, data_dir=None):
        self.userid = userid
        self.password = password
        self.host = host
        self.port = port
        self.dbsid = dbsid
        self.connect_timeout = connect_timeout
        self.charset = charset
        self.as_dict = as_dict
        self.sslmode = sslmode
        self.sslcert = sslcert
        self.sslrootcert = sslrootcert
        self.sslkey = sslkey
        self.sslcrl = sslcrl
        self.data_dir = data_dir

    def __eq__(self, other):
        return (self.userid == other.userid and
                self.password == other.password and
                self.host == other.host and
                self.port == other.port and
                self.dbsid == other.dbsid)

    def __ne__(self, other):
        return not self.__eq__(other)

    def __str__(self):
        return str(self.__dict__)
