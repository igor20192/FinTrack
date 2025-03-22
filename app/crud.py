import logging
from typing import List
from sqlalchemy.ext.asyncio import AsyncSession
from cache import clear_cache, get_cache, set_cache
from schemas import CreditResponse, PlanPerformanceResponse
from sqlalchemy.sql import text
from datetime import date
import time
from models import Plan
import pandas as pd

logger = logging.getLogger(__name__)


async def get_user_credits(db: AsyncSession, user_id: int) -> List[CreditResponse]:
    """
    Fetches credit details for a given user, including payment statistics
    and overdue information.

    Args:
        db (AsyncSession): Asynchronous database session.
        user_id (int): The user's ID.

    Returns:
        List[CreditResponse]: A list of credit records.
    """
    cache_key = f"user_credits:{user_id}"
    cached = await get_cache(cache_key)
    if cached:
        # Преобразуем строки дат обратно в объекты date
        return [
            CreditResponse(
                **{
                    **item,
                    "issuance_date": date.fromisoformat(item["issuance_date"]),
                    "actual_return_date": (
                        date.fromisoformat(item["actual_return_date"])
                        if item["actual_return_date"]
                        else None
                    ),
                    "return_date": date.fromisoformat(item["return_date"]),
                }
            )
            for item in cached
        ]

    query = text(
        """
        SELECT 
            c.id AS credit_id,
            c.issuance_date,
            c.actual_return_date IS NOT NULL AS is_closed,
            c.actual_return_date,
            c.return_date,
            c.body,
            c.percent,
            SUM(p.sum) AS total_payments,
            SUM(CASE WHEN p.type_id = 1 THEN p.sum ELSE 0 END) AS body_payments,
            SUM(CASE WHEN p.type_id = 2 THEN p.sum ELSE 0 END) AS percent_payments,
            CASE 
                WHEN c.actual_return_date IS NULL AND c.return_date < CURDATE() 
                THEN DATEDIFF(CURDATE(), c.return_date) 
                ELSE NULL 
            END AS overdue_days
        FROM Credits c
        LEFT JOIN Payments p ON c.id = p.credit_id
        WHERE c.user_id = :user_id
        GROUP BY c.id
        """
    )

    result = await db.execute(query, {"user_id": user_id})
    rows = result.fetchall()

    credits_list = [
        CreditResponse(
            credit_id=row.credit_id,
            issuance_date=row.issuance_date,
            is_closed=bool(row.is_closed),
            actual_return_date=row.actual_return_date,
            return_date=row.return_date,
            overdue_days=row.overdue_days if not row.is_closed else None,
            body=row.body,
            percent=row.percent,
            total_payments=row.total_payments if row.is_closed else None,
            body_payments=row.body_payments if not row.is_closed else None,
            percent_payments=row.percent_payments if not row.is_closed else None,
        )
        for row in rows
    ]

    # Преобразуем объекты CreditResponse в словари с датами в виде строк
    credits_dict_list = [
        {
            **credit.model_dump(),
            "issuance_date": credit.issuance_date.isoformat(),
            "actual_return_date": (
                credit.actual_return_date.isoformat()
                if credit.actual_return_date
                else None
            ),
            "return_date": credit.return_date.isoformat(),
        }
        for credit in credits_list
    ]

    try:
        await set_cache(cache_key, credits_dict_list)
    except Exception as e:
        logger.error(f"Failed to set cache: {e}")

    return credits_list


async def check_existing_plan(db: AsyncSession, period: date, category_id: int) -> bool:
    """
    Checks if a plan with the given period and category ID already exists in the database.

    Args:
        db: An asynchronous SQLAlchemy session.
        period: The period (date) to check for.
        category_id: The category ID to check for.

    Returns:
        True if a plan with the given period and category ID exists, False otherwise.
    """
    query = text(
        "SELECT COUNT(*) FROM Plans WHERE period = :period AND category_id = :category_id"
    )
    result = await db.execute(query, {"period": period, "category_id": category_id})
    count = result.scalar()
    return count > 0


async def get_category_id(db: AsyncSession, category_name: str) -> int:
    """
    Retrieves the ID of a category from the Dictionary table based on its name.

    Args:
        db: An asynchronous SQLAlchemy session.
        category_name: The name of the category to retrieve the ID for.

    Returns:
        The ID of the category.

    Raises:
        ValueError: If the category with the given name is not found in the Dictionary table.
    """
    query = text("SELECT id FROM Dictionary WHERE name = :name")
    result = await db.execute(query, {"name": category_name})
    category_id = result.scalar()
    if category_id is None:
        raise ValueError(f"Категория '{category_name}' не найдена в таблице Dictionary")
    return category_id


async def insert_plans(db: AsyncSession, df: pd.DataFrame):
    """
    Inserts plans from a Pandas DataFrame into the database.

    Args:
        db: An asynchronous SQLAlchemy session.
        df: A Pandas DataFrame containing plan data with columns 'month', 'category_name', and 'sum'.

    Raises:
        ValueError: If the month in the DataFrame is not the first day of the month,
                    if the category name is not found, or if a plan with the same
                    period and category already exists.
    """
    affected_years = set()

    for _, row in df.iterrows():
        period = pd.to_datetime(row["month"], format="%Y-%m-%d").date()
        if period.day != 1:
            raise ValueError(f"Месяц плана '{period}' должен быть первым числом месяца")

        category_id = await get_category_id(db, row["category_name"])

        # Duplicate check
        if await check_existing_plan(db, period, category_id):
            raise ValueError(
                f"План для месяца {period} и категории {row['category_name']} уже существует"
            )

        plan = Plan(period=period, sum=row["sum"], category_id=category_id)
        db.add(plan)
        affected_years.add(period.year)
    await db.commit()
    logger.info("All plans prepared for insertion")
    # Очистка кэша для всех затронутых годов
    for year in affected_years:
        try:
            await clear_cache(f"year_performance:{year}")
            logger.info(f"Cache cleared for year: {year}")
        except Exception as e:
            logger.error(f"Failed to clear cache for year {year}: {e}")


async def get_plans_performance(db: AsyncSession, check_date: date):
    """
    Retrieves the performance of plans up to a given check date.

    Args:
        db: An asynchronous SQLAlchemy session.
        check_date: The date up to which to check the performance of plans.

    Returns:
        A list of PlanPerformanceResponse objects, each representing the performance of a plan.
    """
    cache_key = f"plans_performance:{check_date.isoformat()}"
    cached = await get_cache(cache_key)
    if cached:
        # Преобразуем строки дат обратно в объекты date
        return [
            PlanPerformanceResponse(
                **{**item, "month": date.fromisoformat(item["month"])}
            )
            for item in cached
        ]

    query = text(
        """
        SELECT 
            p.period AS month,
            d.name AS category,
            p.sum AS plan_sum,
            COALESCE(
                CASE 
                    WHEN d.id = 3 THEN (
                        SELECT SUM(c.body) 
                        FROM Credits c 
                        WHERE c.issuance_date BETWEEN p.period AND :check_date
                    )
                    WHEN d.id = 4 THEN (
                        SELECT SUM(pm.sum) 
                        FROM Payments pm 
                        WHERE pm.payment_date BETWEEN p.period AND :check_date
                    )
                    ELSE 0
                END, 0
            ) AS actual_sum
        FROM Plans p
        JOIN Dictionary d ON p.category_id = d.id
        WHERE p.period <= :check_date
    """
    )
    result = await db.execute(query, {"check_date": check_date})
    rows = result.fetchall()
    plans_list = [
        PlanPerformanceResponse(
            month=row[0],
            category=row[1],
            plan_sum=row[2],
            actual_sum=float(row[3]),
            performance_percent=(
                round(float(row[3]) / row[2] * 100, 2) if row[2] > 0 else 0
            ),
        )
        for row in rows
    ]

    # Convert to serializable format with dates as strings
    plans_dict_list = [
        {**plan.model_dump(), "month": plan.month.isoformat()} for plan in plans_list
    ]

    try:
        await set_cache(cache_key, plans_dict_list)
    except Exception as e:
        logger.error(f"Failed to set cache for {cache_key}: {e}")

    return plans_list


async def fetch_total_issuance(db: AsyncSession, year: int, start_time: float) -> float:
    """Receives the total amount of payments for the year."""
    result = await db.execute(
        text(
            "SELECT COALESCE(SUM(body), 0) FROM Credits WHERE YEAR(issuance_date) = :year"
        ),
        {"year": year},
    )
    total = float(result.scalar() or 0)
    logger.info(f"Total issuance calculated in {time.time() - start_time:.2f} seconds")
    return total


async def fetch_total_collection(
    db: AsyncSession, year: int, start_time: float
) -> float:
    """Receives the total amount of fees for the year."""
    result = await db.execute(
        text(
            "SELECT COALESCE(SUM(sum), 0) FROM Payments WHERE YEAR(payment_date) = :year"
        ),
        {"year": year},
    )
    total = float(result.scalar() or 0)
    logger.info(
        f"Total collection calculated in {time.time() - start_time:.2f} seconds"
    )
    return total


async def fetch_subquery_data(db: AsyncSession, query: str, year: int) -> dict:
    """Executes a subquery and returns the data as a dictionary."""
    result = await db.execute(text(query), {"year": year})
    return {
        row[0]: {"count": row[1], "sum": float(row[2])} for row in result.fetchall()
    }


def build_performance_item(
    row: tuple,
    credits_data: dict,
    payments_data: dict,
    total_issuance: float,
    total_collection: float,
) -> dict:
    """Generates a single performance list item."""
    month = row[3]  # period_month
    plan_issuance_sum = float(row[1] or 0)
    plan_collection_sum = float(row[2] or 0)
    credits = credits_data.get(month, {"count": 0, "sum": 0})
    payments = payments_data.get(month, {"count": 0, "sum": 0})

    return {
        "month_year": row[0].strftime("%Y-%m"),
        "issuance_count": credits["count"],
        "plan_issuance_sum": plan_issuance_sum,
        "actual_issuance_sum": credits["sum"],
        "issuance_performance_percent": (
            round(credits["sum"] / plan_issuance_sum * 100, 2)
            if plan_issuance_sum > 0
            else 0
        ),
        "payment_count": payments["count"],
        "plan_collection_sum": plan_collection_sum,
        "actual_collection_sum": payments["sum"],
        "collection_performance_percent": (
            round(payments["sum"] / plan_collection_sum * 100, 2)
            if plan_collection_sum > 0
            else 0
        ),
        "issuance_percent_of_year": (
            round(credits["sum"] / total_issuance * 100, 2) if total_issuance > 0 else 0
        ),
        "collection_percent_of_year": (
            round(payments["sum"] / total_collection * 100, 2)
            if total_collection > 0
            else 0
        ),
    }


async def get_year_performance(db: AsyncSession, year: int):
    """
    Retrieves the yearly performance data, including issuance, collection, and plan details.

    This function fetches performance data for a given year, combining information from
    Credits, Payments, and Plans tables. It also utilizes caching to improve performance.

    Args:
        db (AsyncSession): The asynchronous database session.
        year (int): The year for which to retrieve performance data.

    Returns:
        list: A list of dictionaries, where each dictionary represents the performance
              data for a month in the given year. Returns an empty list if no data is found.

    Raises:
        Any exceptions raised by the database or cache operations are propagated.
    """
    start_time = time.time()
    cache_key = f"year_performance:{year}"

    # Checking cache
    cached = await get_cache(cache_key)
    if cached:
        logger.info(f"Year performance data fetched from cache for year: {year}")
        return cached

    logger.info(f"Starting year performance query for year: {year}")

    # Getting total amounts
    total_issuance = await fetch_total_issuance(db, year, start_time)
    total_collection = await fetch_total_collection(db, year, start_time)

    # Subqueries
    credits_subquery = """
        SELECT 
            MONTH(issuance_date) AS issuance_month,
            COUNT(DISTINCT id) AS issuance_count,
            COALESCE(SUM(body), 0) AS actual_issuance_sum
        FROM Credits
        WHERE YEAR(issuance_date) = :year
        GROUP BY MONTH(issuance_date)
    """
    payments_subquery = """
        SELECT 
            MONTH(payment_date) AS payment_month,
            COUNT(DISTINCT id) AS payment_count,
            COALESCE(SUM(sum), 0) AS actual_collection_sum
        FROM Payments
        WHERE YEAR(payment_date) = :year
        GROUP BY MONTH(payment_date)
    """
    plans_query = """
        SELECT 
            period AS month_year,
            MAX(CASE WHEN category_id = 3 THEN sum ELSE 0 END) AS plan_issuance_sum,
            MAX(CASE WHEN category_id = 4 THEN sum ELSE 0 END) AS plan_collection_sum,
            MONTH(period) AS period_month
        FROM Plans
        WHERE YEAR(period) = :year
        GROUP BY period
    """

    # Executing subqueries
    credits_data = await fetch_subquery_data(db, credits_subquery, year)
    payments_data = await fetch_subquery_data(db, payments_subquery, year)
    plans_result = await db.execute(text(plans_query), {"year": year})
    rows = plans_result.fetchall()

    logger.info(f"Subqueries executed in {time.time() - start_time:.2f} seconds")

    if not rows:
        logger.info(f"No performance data found for year: {year}")
        await set_cache(cache_key, [])
        return []

    # Formation of the result
    performance_list = [
        build_performance_item(
            row, credits_data, payments_data, total_issuance, total_collection
        )
        for row in rows
    ]

    logger.info(f"Year performance processed in {time.time() - start_time:.2f} seconds")
    await set_cache(cache_key, performance_list)
    return performance_list
