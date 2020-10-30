"""
Handle data modeling for the XBlock
"""
import os
import sqlite3

import pkg_resources


class VerifyQeuryException(Exception):
    """
    Exception thrown when Verification Query has multiple statements
    """
    default_msg = ('Verification query should not contain multiple statemtents'
                   '. Check problem configuration.')

    def __init__(self, *args, msg=None, **kwargs):
        if msg is None:
            msg = self.default_msg
        super().__init__(msg, *args, **kwargs)


class SqlProblem:
    """
    Handle modeling and processing of SQL problems aside from XBlock logic
    """

    dataset = None
    answer_query = None
    verify_query = None
    modification_query = None
    answer_result = None
    is_ordered = True

    # pylint: disable=too-many-arguments
    def __init__(
            self,
            database=None,
            answer_query=None,
            dataset=None,
            verify_query=None,
            modification_query=None,
            is_ordered=True,
    ):
        """
        Initialize variables
        """
        self.database = database
        if dataset:
            self.database = create_database(dataset)
        self.is_ordered = is_ordered
        self.answer_query = answer_query
        self.verify_query = verify_query
        self.modification_query = modification_query
        self.answer_result, _ = SqlProblem.get_query_result(
            self.database,
            answer_query,
            verify_query,
            modification_query,
        )

    def attempt(self, query):
        """
        Attempt to answer the problem with the provided query
        """
        submission_result, error = SqlProblem.get_query_result(
            self.database,
            query,
            self.verify_query,
            self.modification_query,
        )
        comparison = SqlProblem.compare_rows(
            self.answer_result,
            submission_result,
            is_ordered=self.is_ordered,
        )
        return (submission_result, self.answer_result, error, comparison)

    @classmethod
    def create_database(cls, path_sql):
        """
        Create a new in-memory database, initialized via SQL file

        This only needs run once, at startup.
        """
        with open(path_sql, 'r') as sql:
            query = sql.read()
        connection = cls.create_database_from_sql(query)
        return connection

    @classmethod
    def create_database_from_sql(cls, query):
        """
        Create a new in-memory database, initialized via SQL query
        """
        connection = sqlite3.connect(':memory:', check_same_thread=False)
        with connection:
            connection.executescript(query)
        return connection

    @staticmethod
    def clone_database(source):
        """
        Copy the contents of a source database into a new in-memory database

        This should be run for each request.
        """
        destination = sqlite3.connect(':memory:', check_same_thread=False)
        query = ''.join(line for line in source.iterdump())
        destination.executescript(query)
        return destination

    @classmethod
    def get_query_result(cls, source, query, verify_query=None,
                         modification_query=None):
        """
        Execute the provided answer and verification queries against a copy of
        the database, and returns the final result.
        """
        database = cls.clone_database(source)
        result, error = SqlProblem._run(database, query)
        if verify_query:
            if modification_query:
                result, _ = SqlProblem._run(database, modification_query)
            result, error = SqlProblem._run(database, verify_query, True)
        return result, error

    @staticmethod
    def _run(database, query, is_verify_query=False):
        """
        Executes a single SQL query on the database.
        is_verify_query prevents execution of multiple statements.
        """
        def execute_query(connection, query, is_verify_query):
            try:
                # Try execute() before executescript() as executescript
                # doesn't returns rows from SELECT statements
                rows = connection.execute(query)
            except sqlite3.Warning as warning:
                if str(warning).startswith('You can only execute one'):
                    if is_verify_query:
                        # pylint: disable=raise-missing-from
                        raise VerifyQeuryException()
                    rows = connection.executescript(query)
                else:
                    raise warning
            return rows

        result = []
        message = None
        with database as connection:
            try:
                rows = execute_query(connection, query, is_verify_query)
                for row in rows:
                    result.append(row)
            except Exception as error:  # pylint: disable=broad-except
                result = None
                message = str(error)
        return result, message

    @staticmethod
    def compare_rows(expected, actual, is_ordered=True):
        """
        Compare the results of two queries
        """
        expected = expected or []
        actual = actual or []
        if len(expected) != len(actual):
            return False
        if not is_ordered:
            expected = sorted(expected, key=str)
            actual = sorted(actual, key=str)
        comparison = all(
            row_expected == row_actual
            for row_expected, row_actual in zip(expected, actual)
        )
        return comparison


def all_datasets():
    """
    Lookup the names of all avaiable datasets (.sql files)
    """
    dataset_directory = pkg_resources.resource_filename(
        'sql_grader',
        'datasets'
    )
    for _, _, files in os.walk(dataset_directory):
        for fname in files:
            if fname.endswith('.sql'):
                fname = fname[:-4]
                yield fname


def resource_string(path):
    """
    Handy helper for getting resources from our kit
    """
    data = pkg_resources.resource_string(__name__, path)
    return data.decode('utf8')


def create_database(database_name):
    """
    Load a new database from a dataset on disk
    """
    pathname = 'datasets/' + database_name + '.sql'
    contents = resource_string(pathname)
    database = SqlProblem.create_database_from_sql(contents)
    return database
