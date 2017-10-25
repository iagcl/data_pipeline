import collections
import data_pipeline.constants.const as const

TestCase = collections.namedtuple('TestCase', "description input_all_table_results expected_subject expected_total_count expected_status expected_min_lsn expected_max_lsn expected_run_id expected_mailing_list")

tests=[
    TestCase(
        description="Single table success",
        input_all_table_results={
            'tableA': (123, const.SUCCESS, const.INITSYNCEXTRACT, "foo")
        },
        expected_total_count=1,
        expected_status=const.SUCCESS,
        expected_min_lsn=123,
        expected_max_lsn=123,
        expected_run_id=1,
        expected_subject='myprofile InitSync SUCCESS',
        expected_mailing_list=set(['someone@gmail.com']),
    ),

    TestCase(
        description="Single table success, extractlsn disabled",
        input_all_table_results={
            'tableA': (None, const.SUCCESS, const.INITSYNCEXTRACT, "foo")
        },
        expected_total_count=1,
        expected_status=const.SUCCESS,
        expected_min_lsn=None,
        expected_max_lsn=None,
        expected_run_id=1,
        expected_subject='myprofile InitSync SUCCESS',
        expected_mailing_list=set(['someone@gmail.com']),
    ),

    TestCase(
        description="Single table error",
        input_all_table_results={
            'tableA': (123, const.ERROR, const.INITSYNCEXTRACT, "foo")
        },
        expected_total_count=1,
        expected_status=const.ERROR,
        expected_min_lsn=123,
        expected_max_lsn=123,
        expected_run_id=1,
        expected_subject='myprofile InitSync ERROR',
        expected_mailing_list=set(['someone@gmail.com', 'someone@error.com']),
    ),

    TestCase(
        description="Three table success",
        input_all_table_results={
            'tableA': (123, const.SUCCESS, const.INITSYNCEXTRACT, "foo"),
            'tableB': (123, const.SUCCESS, const.INITSYNCEXTRACT, "foo"),
            'tableC': (123, const.SUCCESS, const.INITSYNCEXTRACT, "foo"),
        },
        expected_total_count=3,
        expected_status=const.SUCCESS,
        expected_min_lsn=123,
        expected_max_lsn=123,
        expected_run_id=1,
        expected_subject='myprofile InitSync SUCCESS',
        expected_mailing_list=set(['someone@gmail.com']),
    ),

    TestCase(
        description="Three table error",
        input_all_table_results={
            'tableA': (123, const.ERROR, const.INITSYNCEXTRACT, "foo"),
            'tableB': (123, const.ERROR, const.INITSYNCEXTRACT, "foo"),
            'tableC': (123, const.ERROR, const.INITSYNCEXTRACT, "foo"),
        },
        expected_total_count=3,
        expected_status=const.ERROR,
        expected_min_lsn=123,
        expected_max_lsn=123,
        expected_run_id=1,
        expected_subject='myprofile InitSync ERROR',
        expected_mailing_list=set(['someone@gmail.com', 'someone@error.com']),
    ),

    TestCase(
        description="Error on last table resulting in warning",
        input_all_table_results={
            'tableA': (123, const.SUCCESS, const.INITSYNC, "foo"),
            'tableB': (123, const.SUCCESS, const.INITSYNC, "foo"),
            'tableC': (123, const.SUCCESS, const.INITSYNC, "foo"),
            'tableD': (123, const.SUCCESS, const.INITSYNC, "foo"),
            'tableE': (123, const.ERROR, const.INITSYNCAPPLY, "foo"),
        }, 
        expected_total_count=5,
        expected_status=const.WARNING,
        expected_min_lsn=123,
        expected_max_lsn=123,
        expected_run_id=1,
        expected_subject='myprofile InitSync WARNING (4 out of 5)',
        expected_mailing_list=set(['someone@gmail.com', 'someone@error.com']),
    ),

    TestCase(
        description="Error on second last table resulting in warning",
        input_all_table_results={
            'tableA': (123, const.SUCCESS, const.INITSYNC, "foo"),
            'tableB': (123, const.SUCCESS, const.INITSYNC, "foo"),
            'tableC': (123, const.SUCCESS, const.INITSYNC, "foo"),
            'tableD': (123, const.ERROR, const.INITSYNCAPPLY, "foo"),
            'tableE': (123, const.SUCCESS, const.INITSYNC, "foo"),
        }, 
        expected_total_count=5,
        expected_status=const.WARNING,
        expected_min_lsn=123,
        expected_max_lsn=123,
        expected_run_id=1,
        expected_subject='myprofile InitSync WARNING (4 out of 5)',
        expected_mailing_list=set(['someone@gmail.com', 'someone@error.com']),
    ),

    TestCase(
        description="Error on first table resulting in warning",
        input_all_table_results={
            'tableA': (123, const.ERROR, const.INITSYNCAPPLY, "foo"),
            'tableB': (123, const.SUCCESS, const.INITSYNC, "foo"),
            'tableC': (123, const.SUCCESS, const.INITSYNC, "foo"),
            'tableD': (123, const.SUCCESS, const.INITSYNC, "foo"),
            'tableE': (123, const.SUCCESS, const.INITSYNC, "foo"),
        }, 
        expected_total_count=5,
        expected_status=const.WARNING,
        expected_min_lsn=123,
        expected_max_lsn=123,
        expected_run_id=1,
        expected_subject='myprofile InitSync WARNING (4 out of 5)',
        expected_mailing_list=set(['someone@gmail.com', 'someone@error.com']),
    ),

    TestCase(
        description="Error on middle table resulting in warning",
        input_all_table_results={
            'tableA': (123, const.SUCCESS, const.INITSYNC, "foo"),
            'tableB': (123, const.SUCCESS, const.INITSYNC, "foo"),
            'tableC': (123, const.ERROR, const.INITSYNCAPPLY, "foo"),
            'tableD': (123, const.SUCCESS, const.INITSYNC, "foo"),
            'tableE': (123, const.SUCCESS, const.INITSYNC, "foo"),
        }, 
        expected_total_count=5,
        expected_status=const.WARNING,
        expected_min_lsn=123,
        expected_max_lsn=123,
        expected_run_id=1,
        expected_subject='myprofile InitSync WARNING (4 out of 5)',
        expected_mailing_list=set(['someone@gmail.com', 'someone@error.com']),
    ),
]
