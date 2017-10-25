import os
import data_pipeline.constants.const as const
import data_pipeline.logger.logging_loader as logging_loader


TEST_OUTFILE = 'file.out'


def setup_logging(workdirectory):
    logging_loader.setup_logging(workdirectory)


def mock_get_program_args(mocker, patch_path, mockargv):
    mock_get_program_args = mocker.patch(patch_path)
    mock_get_program_args.return_value = mockargv

def mock_get_prev_run_cdcs(mocker):
    mock_get_schemas_and_tables = mocker.patch('data_pipeline.extractor.cdc_extractor.get_prev_run_cdcs')
    mock_get_schemas_and_tables.return_value = (1, 2)


def mock_get_schemas_and_tables(mocker, schemas, tables, all_tables=None):
    mock_get_schemas_and_tables = mocker.patch('data_pipeline.extractor.extractor.get_schemas_and_tables')
    all_tables = tables if not all_tables else all_tables
    mock_get_schemas_and_tables.return_value = (set(schemas), set(tables), set(all_tables))


def mock_get_inactive_applied_tables(mocker, tables):
    mock_get_inactive_applied_tables = mocker.patch('data_pipeline.applier.applier.get_inactive_applied_tables')
    mock_get_inactive_applied_tables .return_value = tables


def mock_build_kafka_producer(mocker):
    mock_producer_config = {}
    mock_producer = mocker.Mock(**mock_producer_config)

    mock_build_producer = mocker.patch('data_pipeline.extractor.extractor.build_kafka_producer')
    mock_build_producer.return_value = mock_producer
    return mock_producer


def build_mock_audit_factory(mocker):
    pc_config = {'insert.return_value': None, 'update.return_value': None}
    mock_pc = mocker.Mock(**pc_config)

    audit_factory_config = {'build_process_control.return_value': mock_pc}
    mock_audit_factory = mocker.Mock(**audit_factory_config)
    return mock_audit_factory


def get_default_argv_config(tmpdir):
    try:
        p = str(tmpdir.mkdir("test_dir"))
    except Exception, e:
        # If tmpdir already created
        p = os.path.join(str(tmpdir), "test_dir")

    outfile = str(os.path.join(p, TEST_OUTFILE))
    return {"arraysize": 1000,
            "auditschema": "ctl",
            "audituser": "foo/bar@audithost:1234/mydb",
            "sourceuser": "foo/bar@sourcehost:1234/mydb",
            "profilename": "myprofile",
            "profileversion": 1,
            "outputfile": outfile,
            "rawfile": outfile,
            "profileroutputfile": outfile,
            "workdirectory": str(p),
            "sourcedbtype": const.ORACLE,
            "targetdbtype": const.POSTGRES,
            "sourceschema": "sys",
            "targetschema": "ctl",
            "clientencoding": "utf-8",
            "kill": False,
            "datatypemap": "conf/postgres_datatype_mappings.yaml",
            "config": "conf/data_pipeline.yaml",
            "sender": "john.smith@sample.com",
            "recipients": ["john.smith@sample.com"],
            "smtp_server": "localhost",
            "donotapply": False,
            "donotcommit": False,
            "donotload": False,
            "skipbatch": 0,
            "auditcommitpoint": const.MIN_COMMIT_POINT,
            "targetcommitpoint": const.MIN_COMMIT_POINT,
            "bulkapply": False,
            "targetuser": "foo/bar@targethost:1234/mydb",
            "notifysmtpserver": "localhost",
            "notifysender": "someone",
            "notifyerrorlist": ["someone@error.com"],
            "notifysummarylist": ["someone@gmail.com"],
            "sourcedictionary": const.ONLINE_DICT,
            "startscn": None,
            "endscn": None,
            "checkexistingextract": False,
            "inputfile": None,
            "seektoend": False,
            "loaddefinition": const.SRC,
            "nullstring": const.NULL,
            "retrypause": 0.1,
            "retry": 0,
            "metacols": {
                const.METADATA_INSERT_TS_COL: "ctl_ins_ts",
                const.METADATA_UPDATE_TS_COL: "ctl_upd_ts"
            }
        }
