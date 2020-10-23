#!/usr/bin/env python
"""
Test basic XBlock display function
"""
import os

from unittest import TestCase

from sql_grader.problem import SqlProblem


class TestGrading(TestCase):
    """
    Test grading of different types of problems
    """
    def setUp(self):
        current_folder = os.path.dirname(__file__)
        sql_file = "{0}/fixtures/rating.sql".format(current_folder)
        self.database = SqlProblem.create_database(sql_file)

    def test_select(self):
        """
        Test simple SELECT statements
        """
        verify_query = None
        answer_query = "SELECT * FROM Movie order by mID desc;"
        query = "SELECT * FROM Movie order by mID asc;"

        # Verify that comparison is False if is_ordered = True
        submission_result, answer_result, error, comparison = SqlProblem(
            answer_query=answer_query,
            database=self.database,
            verify_query=verify_query,
            is_ordered=True
        ).attempt(query)
        self.assertEqual(error, None)
        self.assertEqual(comparison, False)
        self.assertEqual(submission_result, sorted(answer_result))

        # Verify that comparison is True if is_ordered = False
        submission_result, answer_result, error, comparison = SqlProblem(
            answer_query=answer_query,
            database=self.database,
            verify_query=verify_query,
            is_ordered=False
        ).attempt(query)
        self.assertEqual(error, None)
        self.assertEqual(comparison, True)
        self.assertEqual(submission_result, sorted(answer_result))

        # Verify that comparison is False if different rows are returned
        submission_result, answer_result, error, comparison = SqlProblem(
            answer_query="answer_query",
            database=self.database,
            verify_query=verify_query,
            is_ordered=False
        ).attempt("select * from Movie where mID=101")
        self.assertEqual(error, None)
        self.assertEqual(comparison, False)

        # Verify that comparison is False if different columns are returned
        submission_result, answer_result, error, comparison = SqlProblem(
            answer_query="answer_query",
            database=self.database,
            verify_query=verify_query,
            is_ordered=False
        ).attempt("select mID from Movie where mID")
        self.assertEqual(error, None)
        self.assertEqual(comparison, False)

    def test_update_statements(self):
        """
        Test statements which modify the table
        """
        verify_query = "SELECT * FROM Movie where mID = 1"
        answer_query = "insert into Movie values(1, 'Movie', 2000, 'Director')"
        query = "update Movie \
                 set mID=1, title='Movie', year=2000, director='Director' \
                 where mID=101"

        submission_result, answer_result, error, comparison = SqlProblem(
            answer_query=answer_query,
            database=self.database,
            verify_query=verify_query,
            is_ordered=False
        ).attempt(query)

        self.assertEqual(error, None)
        self.assertEqual(comparison, True)
        self.assertEqual(submission_result, answer_result)
        self.assertEqual(len(submission_result), 1)

    def test_multiple_statements(self):
        """
        Test queries with multiple statements in them
        """
        verify_query = "select * from Movie where mID < 10"
        answer_query = """
        insert into Movie values(1, 'Movie', 2000, 'Director');
        insert into Movie values(2, 'Movie 2', 2000, 'Director 2');
        """
        query = """
        update Movie
        set mID=1, title='Movie', year=2000, director='Director'
        where mID=101;
        insert into Movie values(2, 'Movie 2', 2000, 'Director 2');
        """

        submission_result, answer_result, error, comparison = SqlProblem(
            answer_query=answer_query,
            database=self.database,
            verify_query=verify_query,
            is_ordered=False
        ).attempt(query)

        self.assertEqual(error, None)
        self.assertEqual(comparison, True)
        self.assertEqual(submission_result, answer_result)
        self.assertEqual(len(submission_result), 2)

        verify_query = """
        insert into Movie values(3, 'Movie', 2000, 'Director 3');
        SELECT * FROM Movie where mID < 10;
        """
        submission_result, answer_result, error, comparison = SqlProblem(
            answer_query=answer_query,
            database=self.database,
            verify_query=verify_query,
            is_ordered=False
        ).attempt(query)

        self.assertNotEqual(error, None)

    def test_pre_verification(self):
        """
        Test that pre_verify_query is executed before checking outputs
        """
        answer_query = """
        create trigger change1
        after insert on Movie
        for each row
        when (new.year > 1980 and new.year < 1990)
          begin update Movie
            set title="80s movie" where mID=new.mID; end;

        create trigger change2
        after insert on Movie
        for each row
        when (new.year is NULL)
          begin update Movie
            set director=NULL where mID=new.mID end;
        """
        query = answer_query
        pre_verify_query = """
        insert into Movie values (1, "E.T.", 1982, "Steven Spielberg");
        insert into Movie values (2, null, 1992, "David Fincher");
        """
        verify_query = """
        select * from Movie
        where title like '80s %' or title IS NULL
        """

        submission_result, answer_result, error, comparison = SqlProblem(
            answer_query=answer_query,
            database=self.database,
            verify_query=verify_query,
            pre_verify_query=pre_verify_query,
            is_ordered=False
        ).attempt(query)
        self.assertEqual(error, None)
        self.assertEqual(comparison, True)
        self.assertEqual(submission_result, answer_result)
        self.assertEqual(submission_result, [
            (1, '80s movie', 1982, 'Steven Spielberg'),
            (2, None, 1992, 'David Fincher')
        ])

    def test_error(self):
        """
        Test that invalid queries produce error
        """
        verify_query = None
        answer_query = "SELECT * FROM Movie;"
        query = "Not a SQL Query;"

        # Verify that comparison returns False if is_ordered = True
        _, _, error, _ = SqlProblem(
            answer_query=answer_query,
            database=self.database,
            verify_query=verify_query,
            is_ordered=True
        ).attempt(query)

        self.assertNotEqual(error, None)
