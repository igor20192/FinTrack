# tests/test_crud.py
import time
import unittest
from unittest.mock import AsyncMock, patch
from datetime import date
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
        # Create a temporary SQLite database in memory
        self.engine = create_async_engine("sqlite+aiosqlite:///:memory:", echo=False)
        self.async_session = sessionmaker(
            self.engine, class_=AsyncSession, expire_on_commit=False
        )

        # Create tables
        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

        # Fill in the test data
        async with self.async_session() as session:
            # Dictionary
            session.add_all(
                [
                    Dictionary(id=1, name="Body"),
                    Dictionary(id=2, name="Percent"),
                    Dictionary(id=3, name="Issuance"),
                    Dictionary(id=4, name="Collection"),
                ]
            )
            # Credits
            session.add_all(
                [
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
                ]
            )
            # Payments
            session.add_all(
                [
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
                ]
            )
            # Plans
            session.add_all(
                [
                    Plan(id=1, period=date(2021, 1, 1), sum=10000, category_id=3),
                    Plan(id=2, period=date(2021, 1, 1), sum=5000, category_id=4),
                ]
            )
            await session.commit()

    async def asyncTearDown(self):
        # Delete tables after tests
        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.drop_all)
        await self.engine.dispose()

    @patch("app.crud.get_cache", new_callable=AsyncMock)
    @patch("app.crud.set_cache", new_callable=AsyncMock)
    async def test_get_year_performance(self, mock_set_cache, mock_get_cache):
        # Mock the cache so that it returns None (no data in cache)
        mock_get_cache.return_value = None

        async with self.async_session() as db:
            result = await get_year_performance(db, 2021)

            # Checking the result
            self.assertEqual(len(result), 1)  # Одна запись для января
            self.assertEqual(result[0]["month_year"], "2021-01")
            self.assertEqual(result[0]["issuance_count"], 1)  # Один кредит в январе
            self.assertEqual(result[0]["plan_issuance_sum"], 10000)
            self.assertEqual(result[0]["actual_issuance_sum"], 10000)
            self.assertEqual(result[0]["issuance_performance_percent"], 100.0)
            self.assertEqual(result[0]["payment_count"], 0)  # Нет платежей в январе
            mock_set_cache.assert_called_once()

    async def test_insert_plans(self):
        # Test data
        df = pd.DataFrame(
            {"month": ["2021-02-01"], "category_name": ["Issuance"], "sum": [2000]}
        )

        async with self.async_session() as db:
            await insert_plans(db, df)
            # Check that the plan has been added
            result = await db.execute(
                select(Plan).where(Plan.period == date(2021, 2, 1))
            )
            plan = result.scalar_one()
            self.assertEqual(plan.sum, 2000)
            self.assertEqual(plan.category_id, 3)

    @patch("app.crud.get_cache", new_callable=AsyncMock)
    @patch("app.crud.set_cache", new_callable=AsyncMock)
    async def test_get_credits_with_payments_orm(self, mock_set_cache, mock_get_cache):
        # Mock the cache
        mock_get_cache.return_value = None

        async with self.async_session() as db:
            result = await get_user_credits(db, 1)

            # Checking the result
            self.assertEqual(len(result), 2)  # Two credits for user_id=1
            credit_open = next(c for c in result if c.credit_id == 1)
            credit_closed = next(c for c in result if c.credit_id == 2)

            # Open credit
            self.assertFalse(credit_open.is_closed)
            self.assertIsNotNone(
                credit_open.overdue_days
            )  # Expired (2021-12-31 < today)
            self.assertEqual(credit_open.total_payments, None)
            self.assertEqual(credit_open.body_payments, 5000.0)
            self.assertEqual(credit_open.percent_payments, 250.0)

            # Closed loan
            self.assertTrue(credit_closed.is_closed)
            self.assertIsNone(credit_closed.overdue_days)
            self.assertIsNotNone(credit_closed.total_payments)

            mock_set_cache.assert_called_once()

    async def test_fetch_total_issuance(self):
        async with self.async_session() as db:
            total = await fetch_total_issuance(db, 2021, time.time())
            self.assertEqual(total, 15000.0)  # 10000 + 5000

    async def test_fetch_total_collection(self):
        async with self.async_session() as db:
            total = await fetch_total_collection(db, 2021, time.time())
            self.assertEqual(total, 10250.0)  # 5000 + 250 + 4750 + 250


if __name__ == "__main__":
    unittest.main()
