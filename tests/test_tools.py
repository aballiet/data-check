"""
Tests for tools module.
"""

import pytest
from unittest.mock import Mock, patch
from concurrent.futures import ThreadPoolExecutor

from data_check.tools import run_multithreaded


class TestTools:
    """Test utility functions in tools module."""

    def test_run_multithreaded_single_job(self):
        """Test run_multithreaded with a single job."""
        def test_func(x, y):
            return x + y

        jobs = [(test_func, {'x': 1, 'y': 2})]
        results = run_multithreaded(jobs, max_workers=1)

        assert results == [3]

    def test_run_multithreaded_multiple_jobs(self):
        """Test run_multithreaded with multiple jobs."""
        def test_func(x, y):
            return x + y

        jobs = [
            (test_func, {'x': 1, 'y': 2}),
            (test_func, {'x': 3, 'y': 4}),
            (test_func, {'x': 5, 'y': 6})
        ]
        results = run_multithreaded(jobs, max_workers=2)

        assert results == [3, 7, 11]

    def test_run_multithreaded_with_exception(self):
        """Test run_multithreaded when a job raises an exception."""
        def test_func(x):
            if x == 2:
                raise ValueError("Test exception")
            return x * 2

        jobs = [
            (test_func, {'x': 1}),
            (test_func, {'x': 2}),
            (test_func, {'x': 3})
        ]

        with pytest.raises(ValueError, match="Test exception"):
            run_multithreaded(jobs, max_workers=2)

    def test_run_multithreaded_empty_jobs(self):
        """Test run_multithreaded with empty jobs list."""
        results = run_multithreaded([], max_workers=1)
        assert results == []

    def test_run_multithreaded_max_workers(self):
        """Test run_multithreaded with different max_workers values."""
        def test_func(x):
            return x * 2

        jobs = [(test_func, {'x': i}) for i in range(5)]

        # Test with max_workers=1
        results_1 = run_multithreaded(jobs, max_workers=1)
        assert results_1 == [0, 2, 4, 6, 8]

        # Test with max_workers=3
        results_3 = run_multithreaded(jobs, max_workers=3)
        assert results_3 == [0, 2, 4, 6, 8]

    @patch('data_check.tools.add_script_run_ctx')
    def test_run_multithreaded_script_context(self, mock_add_script_run_ctx):
        """Test that script run context is added to threads."""
        def test_func(x):
            return x * 2

        jobs = [(test_func, {'x': 1})]
        results = run_multithreaded(jobs, max_workers=1)

        assert results == [2]
        # Verify that add_script_run_ctx was called
        mock_add_script_run_ctx.assert_called()
