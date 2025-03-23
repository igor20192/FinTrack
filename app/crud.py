# app/crud.py
import time
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql import text
from sqlalchemy import extract, select, func
import logging
from typing import List
import pandas as pd
from datetime import date
from cache import get_cache, set_cache, clear_cache
from models import Credit, Plan, Dictionary, Payment
from schemas import CreditResponse, PlanPerformanceResponse

logger = logging.getLogger(__name__)


# ORM functions
async def get_category_id(db: AsyncSession, category_name: str) -> int:
    """Fetches category ID by name using ORM."""
    result = await db.execute(
        select(Dictionary.id).where(Dictionary.name == category_name)
    )
    category_id = result.scalar()
    if not category_id:
        raise ValueError(f"Category '{category_name}' not found")
    return category_id


async def check_existing_plan(db: AsyncSession, period: date, category_id: int) -> bool:
    """Checks if a plan exists for the given period and category using ORM."""
    result = await db.execute(
        select(func.count())
        .select_from(Plan)
        .where(Plan.period == period, Plan.category_id == category_id)
    )
    return result.scalar() > 0


async def insert_plans(db: AsyncSession, df: pd.DataFrame):
    """Inserts plans from a DataFrame into the database using ORM and clears cache."""
    affected_years = set()
    for _, row in df.iterrows():
        period = pd.to_datetime(row["month"], format="%Y-%m-%d").date()
        if period.day != 1:
            raise ValueError(
                f"Plan month '{period}' must be the first day of the month"
            )

        category_id = await get_category_id(db, row["category_name"])
        if await check_existing_plan(db, period, category_id):
            raise ValueError(
                f"Plan for {period} and category {row['category_name']} already exists"
            )

        plan = Plan(period=period, sum=row["sum"], category_id=category_id)
        db.add(plan)
        affected_years.add(period.year)

    await db.commit()
    logger.info("All plans prepared and inserted into database")

    for year in affected_years:
        try:
            await clear_cache(f"year_performance:{year}")
            await clear_cache(f"plans_performance:*-{year}-*")
            logger.info(
                f"Cache cleared for year_performance and plans_performance: {year}"
            )
        except Exception as e:
            logger.error(f"Failed to clear cache for year {year}: {e}")


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
        # Convert date strings back to date objects
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

    # Leaving SQL for complex aggregation
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

    # Convert CreditResponse objects to dictionaries with dates as strings
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


async def fetch_total_issuance(db: AsyncSession, year: int, start_time: float) -> float:
    """
    Asynchronously fetches the total issuance amount for a given year from the Credits table.

    This function constructs and executes an asynchronous SQLAlchemy query to calculate the sum of the 'body' column
    from the Credits table, filtered by the year extracted from the 'issuance_date' column. It handles potential
    NULL values by replacing them with 0.0 and logs the execution time.

    Args:
        db (AsyncSession): The asynchronous database session to use for executing the query.
        year (int): The year for which to calculate the total issuance amount.
        start_time (float): The timestamp representing the start time of the operation, used for logging performance.

    Returns:
        float: The total issuance amount for the specified year, or 0.0 if no data is found.
    """
    result = await db.execute(
        select(func.coalesce(func.sum(Credit.body), 0.0)).filter(
            extract("year", Credit.issuance_date) == year
        )
    )
    total = float(result.scalar() or 0)
    logger.info(f"Total issuance calculated in {time.time() - start_time:.2f} seconds")
    return total


async def fetch_total_collection(
    db: AsyncSession, year: int, start_time: float
) -> float:
    """
    Asynchronously fetches the total collection amount for a given year from the Payments table.

    This function constructs and executes an asynchronous SQLAlchemy query to calculate the sum of the 'sum' column
    from the Payments table, filtered by the year extracted from the 'payment_date' column. It handles potential
    NULL values by replacing them with 0.0 and logs the execution time.

    Args:
        db (AsyncSession): The asynchronous database session to use for executing the query.
        year (int): The year for which to calculate the total collection amount.
        start_time (float): The timestamp representing the start time of the operation, used for logging performance.

    Returns:
        float: The total collection amount for the specified year, or 0.0 if no data is found.
    """
    result = await db.execute(
        select(func.coalesce(func.sum(Payment.sum), 0.0)).filter(
            extract("year", Payment.payment_date) == year
        )
    )
    total = float(result.scalar() or 0)
    logger.info(
        f"Total collection calculated in {time.time() - start_time:.2f} seconds"
    )
    return total


async def fetch_subquery_data(db: AsyncSession, query: str, year: int) -> dict:
    """
    Executes a subquery and returns the results as a dictionary.

    This function executes an asynchronous SQL subquery against the database, using the provided query string and year parameter.
    It then processes the query results into a dictionary where the keys are the first column of each row (assumed to be a month identifier)
    and the values are dictionaries containing 'count' and 'sum' from the second and third columns of each row, respectively.

    Args:
        db (AsyncSession): The asynchronous database session.
        query (str): The SQL subquery to execute.
        year (int): The year parameter to be used in the subquery.

    Returns:
        dict: A dictionary where keys are month identifiers and values are dictionaries containing 'count' and 'sum' for each month.
              Returns an empty dictionary if the subquery returns no rows.
    """
    result = await db.execute(text(query), {"year": year})
    return {
        row[0]: {"count": row[1], "sum": float(row[2])} for row in result.fetchall()
    }


def build_performance_item(
    row,
    credits_data: dict,
    payments_data: dict,
    total_issuance: float,
    total_collection: float,
) -> dict:
    """
    Constructs a dictionary representing performance data for a specific month.

    This function takes a row of data, pre-processed credits and payments data,
    and total issuance and collection amounts for the year, and assembles them
    into a structured dictionary containing monthly performance metrics.

    Args:
        row: A row of data containing month, plan issuance sum, plan collection sum, and month number.
        credits_data (dict): A dictionary mapping month numbers to credit counts and sums.
        payments_data (dict): A dictionary mapping month numbers to payment counts and sums.
        total_issuance (float): The total issuance amount for the year.
        total_collection (float): The total collection amount for the year.

    Returns:
        dict: A dictionary containing performance data for the specified month, including:
            - month_year (str): The year and month in "YYYY-MM" format.
            - issuance_count (int): The number of credit issuances.
            - plan_issuance_sum (float): The planned issuance sum.
            - actual_issuance_sum (float): The actual issuance sum.
            - issuance_performance_percent (float): The percentage of actual issuance against planned issuance.
            - payment_count (int): The number of payments.
            - plan_collection_sum (float): The planned collection sum.
            - actual_collection_sum (float): The actual collection sum.
            - collection_performance_percent (float): The percentage of actual collection against planned collection.
            - issuance_percent_of_year (float): The percentage of monthly issuance against total yearly issuance.
            - collection_percent_of_year (float): The percentage of monthly collection against total yearly collection.
    """
    month = row[3]
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
    cached = await get_cache(cache_key)
    if cached:
        return cached
    logger.info(f"Starting year performance query for year: {year}")

    total_issuance = await fetch_total_issuance(db, year, start_time)
    total_collection = await fetch_total_collection(db, year, start_time)

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

    credits_data = await fetch_subquery_data(db, credits_subquery, year)
    payments_data = await fetch_subquery_data(db, payments_subquery, year)
    plans_result = await db.execute(text(plans_query), {"year": year})
    rows = plans_result.fetchall()

    logger.info(f"Subqueries executed in {time.time() - start_time:.2f} seconds")

    if not rows:
        logger.info(f"No performance data found for year: {year}")
        await set_cache(cache_key, [])
        return []

    performance_list = [
        build_performance_item(
            row, credits_data, payments_data, total_issuance, total_collection
        )
        for row in rows
    ]

    logger.info(f"Year performance processed in {time.time() - start_time:.2f} seconds")
    await set_cache(cache_key, performance_list)
    return performance_list


async def get_plans_performance(
    db: AsyncSession, check_date: date
) -> List[PlanPerformanceResponse]:
    """
    Retrieves the performance of plans up to a given check date.

    This function fetches plan performance data from the database, comparing planned sums against actual sums
    calculated from Credits and Payments tables up to the specified check date. It also utilizes caching
    to improve performance.

    Args:
        db (AsyncSession): The asynchronous database session.
        check_date (date): The date up to which plan performance is evaluated.

    Returns:
        List[PlanPerformanceResponse]: A list of PlanPerformanceResponse objects, each representing the performance
                                       of a plan up to the check date.

    Raises:
        Exception: If there is an issue setting the cache.
    """
    cache_key = f"plans_performance:{check_date.isoformat()}"
    cached = await get_cache(cache_key)
    if cached:
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

    plans_dict_list = [
        {**plan.model_dump(), "month": plan.month.isoformat()} for plan in plans_list
    ]
    try:
        await set_cache(cache_key, plans_dict_list)
    except Exception as e:
        logger.error(f"Failed to set cache for {cache_key}: {e}")

    return plans_list
