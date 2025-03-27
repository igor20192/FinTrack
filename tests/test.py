# tests/test_crud.py
import time
import unittest
from unittest.mock import AsyncMock, patch
from datetime import date, timedelta
from sqlalchemy import select
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from app.models import Base, Credit, Payment, Plan, Dictionary
from app.crud import (
    get_year_performance,
    insert_plans,
    get_user_credits,
    fetch_total_issuance,
    fetch_total_collection,
)

import pandas as pd


class TestCrud(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        """
        Setting up the test database before each test.
        Creates a temporary SQLite database in memory,
        creates tables, and fills them with test data.
        """
        self.engine = create_async_engine("sqlite+aiosqlite:///:memory:", echo=False)
        self.async_session = sessionmaker(
            self.engine, class_=AsyncSession, expire_on_commit=False
        )

        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

        async with self.async_session() as session:
            # Dictionaries
            session.add_all(
                [
                    Dictionary(id=1, name="Credit Body"),
                    Dictionary(id=2, name="Interest"),
                    Dictionary(id=3, name="Issuance"),
                    Dictionary(id=4, name="Collection"),
                ]
            )
            # Credits
            self.credits = [
                Credit(
                    id=1,
                    user_id=1,
                    issuance_date=date(2021, 1, 15),
                    return_date=date(2021, 12, 31),
                    actual_return_date=None,
                    body=10000,
                    percent=5.0,
                ),
                Credit(
                    id=2,
                    user_id=1,
                    issuance_date=date(2021, 2, 1),
                    return_date=date(2021, 11, 30),
                    actual_return_date=date(2021, 11, 25),
                    body=5000,
                    percent=4.5,
                ),
                Credit(
                    id=3,
                    user_id=2,
                    issuance_date=date(2022, 3, 10),
                    return_date=date(2022, 12, 31),
                    actual_return_date=None,
                    body=2000,
                    percent=6.0,
                ),
                Credit(
                    id=4,
                    user_id=2,
                    issuance_date=date(2022, 4, 1),
                    return_date=date(2023, 3, 31),
                    actual_return_date=None,
                    body=3000,
                    percent=7.0,
                ),
            ]
            session.add_all(self.credits)
            # Payments
            self.payments = [
                Payment(
                    id=1,
                    credit_id=1,
                    payment_date=date(2021, 6, 1),
                    type_id=1,
                    sum=5000,
                ),
                Payment(
                    id=2,
                    credit_id=1,
                    payment_date=date(2021, 7, 1),
                    type_id=2,
                    sum=250,
                ),
                Payment(
                    id=3,
                    credit_id=2,
                    payment_date=date(2021, 7, 1),
                    type_id=2,
                    sum=250,
                ),
                Payment(
                    id=4,
                    credit_id=2,
                    payment_date=date(2021, 8, 1),
                    type_id=1,
                    sum=4750,
                ),
                Payment(
                    id=5,
                    credit_id=3,
                    payment_date=date(2022, 4, 1),
                    type_id=1,
                    sum=1000,
                ),
                Payment(
                    id=6,
                    credit_id=4,
                    payment_date=date(2022, 5, 1),
                    type_id=1,
                    sum=1500,
                ),
            ]
            session.add_all(self.payments)
            # Plans
            self.plans = [
                Plan(id=1, period=date(2021, 1, 1), sum=10000, category_id=3),
                Plan(id=2, period=date(2021, 1, 1), sum=5000, category_id=4),
                Plan(id=3, period=date(2022, 3, 1), sum=3000, category_id=3),
                Plan(id=4, period=date(2022, 4, 1), sum=1000, category_id=4),
            ]
            session.add_all(self.plans)
            await session.commit()

    async def asyncTearDown(self):
        """
        Clearing the test database after each test.
        Deletes all tables and closes the database connection.
        """
        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.drop_all)
        await self.engine.dispose()

    @patch("app.crud.get_cache", new_callable=AsyncMock)
    @patch("app.crud.set_cache", new_callable=AsyncMock)
    async def test_get_year_performance(self, mock_set_cache, mock_get_cache):
        """
        Tests the get_year_performance function for 2021.
        Checks that the data for January 2021 is calculated correctly.
        """
        mock_get_cache.return_value = None  # Simulate no data in the cache

        async with self.async_session() as db:
            result = await get_year_performance(db, 2021)

            self.assertEqual(len(result), 1)  # One record for January
            self.assertEqual(result[0]["month_year"], "2021-01")
            self.assertEqual(result[0]["issuance_count"], 1)  # One credit in January
            self.assertEqual(result[0]["plan_issuance_sum"], 10000)
            self.assertEqual(result[0]["actual_issuance_sum"], 10000)
            self.assertEqual(result[0]["issuance_performance_percent"], 100.0)
            self.assertEqual(result[0]["payment_count"], 0)  # No payments in January
            mock_set_cache.assert_called_once()  # Check that set_cache was called

    @patch("app.crud.get_cache", new_callable=AsyncMock)
    @patch("app.crud.set_cache", new_callable=AsyncMock)
    async def test_get_year_performance_multiple_months(
        self, mock_set_cache, mock_get_cache
    ):
        """
        Tests the get_year_performance function for 2022.
        Checks that the data for March and April 2022 is calculated correctly.
        """
        mock_get_cache.return_value = None
        async with self.async_session() as db:
            result = await get_year_performance(db, 2022)
            self.assertEqual(len(result), 2)
            self.assertEqual(result[0]["month_year"], "2022-03")
            self.assertEqual(result[0]["issuance_count"], 1)
            self.assertEqual(result[0]["plan_issuance_sum"], 3000)
            self.assertEqual(result[0]["actual_issuance_sum"], 2000)
            self.assertEqual(result[0]["issuance_performance_percent"], 66.67)
            self.assertEqual(result[0]["payment_count"], 0)
            self.assertEqual(result[0]["plan_collection_sum"], 0)
            self.assertEqual(result[0]["actual_collection_sum"], 0)
            self.assertEqual(result[0]["collection_performance_percent"], 0)

            self.assertEqual(result[1]["month_year"], "2022-04")
            self.assertEqual(result[1]["issuance_count"], 1)
            self.assertEqual(result[1]["plan_issuance_sum"], 0)
            self.assertEqual(result[1]["actual_issuance_sum"], 3000)
            self.assertEqual(result[1]["issuance_performance_percent"], 0)
            self.assertEqual(result[1]["payment_count"], 1)
            self.assertEqual(result[1]["plan_collection_sum"], 1000)
            self.assertEqual(result[1]["actual_collection_sum"], 1000)
            self.assertEqual(result[1]["collection_performance_percent"], 100.0)
            mock_set_cache.assert_called()

    async def test_insert_plans(self):
        """
        Tests the insert_plans function.
        Checks that the plan is successfully added to the database.
        """
        df = pd.DataFrame(
            {"month": ["2021-02-01"], "category_name": ["Issuance"], "sum": [2000]}
        )

        async with self.async_session() as db:
            await insert_plans(db, df)
            result = await db.execute(
                select(Plan).where(Plan.period == date(2021, 2, 1))
            )
            plan = result.scalar_one()
            self.assertEqual(plan.sum, 2000)
            self.assertEqual(plan.category_id, 3)

    @patch("app.crud.get_cache", new_callable=AsyncMock)
    @patch("app.crud.set_cache", new_callable=AsyncMock)
    async def test_get_credits_with_payments_orm(self, mock_set_cache, mock_get_cache):
        """
        Tests the get_user_credits function.
        Checks that correct credit data is returned for the user with user_id=1.
        """
        mock_get_cache.return_value = None

        async with self.async_session() as db:
            result = await get_user_credits(db, 1)

            self.assertEqual(len(result), 2)  # Two credits for user_id=1
            credit_open = next(c for c in result if c.credit_id == 1)
            credit_closed = next(c for c in result if c.credit_id == 2)

            # Open credit
            self.assertFalse(credit_open.is_closed)
            self.assertIsNotNone(
                credit_open.overdue_days
            )  # Overdue (2021-12-31 < today)
            self.assertEqual(credit_open.total_payments, None)
            self.assertEqual(credit_open.body_payments, 5000.0)
            self.assertEqual(credit_open.percent_payments, 250.0)

            # Closed credit
            self.assertTrue(credit_closed.is_closed)
            self.assertIsNone(credit_closed.overdue_days)
            self.assertIsNotNone(credit_closed.total_payments)

            mock_set_cache.assert_called_once()

    @patch("app.crud.get_cache", new_callable=AsyncMock)
    @patch("app.crud.set_cache", new_callable=AsyncMock)
    async def test_get_credits_with_payments_orm_no_credits(
        self, mock_set_cache, mock_get_cache
    ):
        """
        Tests the get_user_credits function for a user with no credits.
        Checks that an empty list is returned.
        """
        mock_get_cache.return_value = None
        async with self.async_session() as db:
            result = await get_user_credits(db, 999)
            self.assertEqual(len(result), 0)
            mock_set_cache.assert_called_once()

    async def test_fetch_total_issuance(self):
        """
        Tests the fetch_total_issuance function for 2021.
        Checks that the total amount of issued credits is calculated correctly.
        """
        async with self.async_session() as db:
            total = await fetch_total_issuance(db, 2021, time.time())
            self.assertEqual(total, 15000.0)  # 10000 + 5000

    async def test_fetch_total_issuance_different_year(self):
        """
        Tests the fetch_total_issuance function for 2022.
        Checks that the total amount of issued credits is calculated correctly.
        """
        async with self.async_session() as db:
            total = await fetch_total_issuance(db, 2022, time.time())
            self.assertEqual(total, 5000.0)  # 2000 + 3000

    async def test_fetch_total_collection(self):
        """
        Tests the fetch_total_collection function for 2021.
        Checks that the total amount of collected funds is calculated correctly.
        """
        async with self.async_session() as db:
            total = await fetch_total_collection(db, 2021, time.time())
            self.assertEqual(total, 10250.0)  # 5000 + 250 + 4750 + 250

    async def test_fetch_total_collection_different_year(self):
        """
        Tests the fetch_total_collection function for 2022.
        Checks that the total amount of collected funds is calculated correctly.
        """
        async with self.async_session() as db:
            total = await fetch_total_collection(db, 2022, time.time())
            self.assertEqual(total, 2500.0)  # 1000 + 1500


if __name__ == "__main__":
    unittest.main()
