import os
import sys
import pytest
from unittest.mock import patch, Mock
import hashlib

import pandas as pd
import wrds
from taqy.usequity import (
    connect_to_wrds,
    get_wrds_connection,
    set_default_connection,
    cached_sql as real_cached_sql,
)

# License: GPLv3 or later
# Copyright 2025 by Brian K. Boonstra


def pytest_addoption(parser):
    parser.addoption(
        "--update-gold",
        action="store_true",
        default=False,
        help="Update the gold files instead of asserting against them.",
    )
    parser.addoption(
        "--avoid-db-cache",
        action="store_true",
        default=False,
        help="Use real database queries.",
    )
    parser.addoption(
        "--write-db-cache",
        action="store_true",
        default=False,
        help="Use real database queries and update cache on disk.",
    )


@pytest.fixture(scope="module")
def use_db_cache(request) -> bool:
    """
    Returns True if we should read from the cached data on disk
    (i.e., NOT updating the db cache), or False if we should do real queries.
    """
    return not (
        request.config.getoption("--avoid-db-cache")
        or request.config.getoption("--write-db-cache")
    )


@pytest.fixture(scope="module")
def write_db_cache(request) -> bool:
    """
    Returns True if we should read from the cached data on disk
    (i.e., NOT updating the db cache), or False if we should do real queries.
    """
    return request.config.getoption("--write-db-cache")


@pytest.fixture(scope="module", autouse=True)
def db_cache_dir(tmp_path_factory) -> str:
    return os.path.join(os.path.dirname(__file__), "mock_sql_responses")


@pytest.fixture(scope="module", autouse=True)
def mock_or_call_cached_sql(use_db_cache, db_cache_dir, write_db_cache):
    """
    If --update-db-cache is not specified, we mock cached_sql() calls
    to return data from the filesystem. Otherwise, we use the real calls
    and then save the results to the filesystem.

    This fixture is autouse=True so that any test in this module or submodules
    that requires cached_sql automatically goes through this mechanism.
    """

    if use_db_cache:
        print("Mocking cached_sql", file=sys.stderr)
        field_names_mocker = Mock()
        field_names_mocker.name = ["time_m_nano"]
        connection_mocker = Mock()
        connection_mocker.describe_table.return_value = field_names_mocker
        # Mock out cached_sql to return data from the filesystem instead of querying the DB.
        with patch("taqy.usequity.get_wrds_connection", return_value=connection_mocker):
            with patch("taqy.usequity.cached_sql") as mock_sql:

                def _mocked_cached_sql(db: None, sql: str):
                    sql_hash = hashlib.md5(sql.encode("utf-8")).hexdigest()
                    cache_file = os.path.join(db_cache_dir, f"{sql_hash}.parquet")
                    if not os.path.isfile(cache_file):
                        raise FileNotFoundError(
                            f"Expected cache file not found: {cache_file}"
                        )
                    return pd.read_parquet(cache_file)

                mock_sql.side_effect = _mocked_cached_sql
                yield
    else:
        print("Wrapping cached_sql", file=sys.stderr)
        wrds_username = os.environ.get("WRDS_USERNAME")
        wrds_password = os.environ.get("WRDS_PASSWORD")
        if wrds_password:
            print(
                f"Found WRDS_PASSWORD in environment, trying to use that with user {wrds_username}",
                file=sys.stderr,
            )
            # As of v3.3.0 The WRDS Python API does not directly support passwords, but we can hack it
            manual_connection = wrds.sql.Connection(
                autoconnect=False, verbose=False, wrds_username=wrds_username
            )
            manual_connection._password = wrds_password
            manual_connection.connect()
            set_default_connection(manual_connection)
        else:
            print(
                f"No WRDS_PASSWORD in environment, using standard connection routines",
                file=sys.stderr,
            )
            connect_to_wrds(wrds_username=wrds_username)

        if write_db_cache:

            def _wrapped_cached_sql(db: wrds.sql.Connection | None, sql: str):
                df = real_cached_sql(db=db or get_wrds_connection(), sql=sql)
                sql_hash = hashlib.md5(sql.encode("utf-8")).hexdigest()
                cache_file = os.path.join(db_cache_dir, f"{sql_hash}.parquet")
                print(
                    f"Writing DB cache file {cache_file}",
                    file=sys.stderr,
                )
                df.to_parquet(cache_file, index=False)
                return df

        else:
            print("Skipped saving DB cache", file=sys.stderr)

            def _wrapped_cached_sql(db: wrds.sql.Connection | None, sql: str):
                df = real_cached_sql(db=db or get_wrds_connection(), sql=sql)
                return df

        with patch("taqy.usequity.cached_sql", side_effect=_wrapped_cached_sql):
            yield
