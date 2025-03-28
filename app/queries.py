# app/queries.py
from datetime import date
from typing import List, Tuple
from sqlalchemy import Integer, Select, null, select, func, case
from sqlalchemy.sql import distinct
from sqlalchemy.ext.asyncio import AsyncSession
from models import Credit, Dictionary, Payment, Plan


def current_date(dialect):
    """
    Returns the SQL expression to get the current date based on the database dialect.

    :param dialect: A string representing the database dialect (e.g., "sqlite", "postgresql").
    :return: An SQL function object representing the current date.
    """
    if dialect == "sqlite":
        return func.date("now")
    return func.curdate()


def date_diff(end_date, start_date, dialect):
    """
    Returns the SQL expression to calculate the difference in days between two dates
    based on the database dialect.

    :param end_date: An SQL expression object representing the end date.
    :param start_date: An SQL expression object representing the start date.
    :param dialect: A string representing the database dialect (e.g., "sqlite", "postgresql").
    :return: An SQL expression object representing the difference in days between end_date and start_date.
    """
    if dialect == "sqlite":
        return func.cast(func.julianday(end_date) - func.julianday(start_date), Integer)
    return func.datediff(end_date, start_date)


# ORM queries for get_year_performance


async def get_credits_data(year: int) -> Select:
    """
    Asynchronously retrieves credit issuance data grouped by month for a given year.

    This function constructs and returns an SQLAlchemy select statement that fetches the following data from the Credit table:
    - The month of issuance (issuance_month)
    - The count of distinct credit IDs for each month (issuance_count)
    - The sum of credit bodies for each month, with NULL values replaced by 0 (actual_issuance_sum)

    The results are filtered by the provided year and grouped by the issuance month.

    Args:
        year (int): The year for which to retrieve credit issuance data.

    Returns:
        Select: An SQLAlchemy select statement that can be executed to fetch the data.
    """

    issuance_month = func.extract("month", Credit.issuance_date).label("issuance_month")

    return (
        select(
            issuance_month,
            func.count(distinct(Credit.id)).label("issuance_count"),
            func.coalesce(func.sum(Credit.body), 0).label("actual_issuance_sum"),
        )
        .where(func.extract("year", Credit.issuance_date) == year)
        .group_by(issuance_month)
    )


async def get_payments_data(year: int) -> Select:
    """
    Asynchronously retrieves payment data grouped by month for a given year.

    This function constructs and returns an SQLAlchemy select statement that fetches the following data from the Payment table:
    - The month of payment (payment_month)
    - The count of distinct payment IDs for each month (payment_count)
    - The sum of payment amounts for each month, with NULL values replaced by 0 (actual_collection_sum)

    The results are filtered by the provided year and grouped by the payment month.

    Args:
        year (int): The year for which to retrieve payment data.

    Returns:
        Select: An SQLAlchemy select statement that can be executed to fetch the data.
    """

    payment_month = func.extract("month", Payment.payment_date).label("payment_month")

    return (
        select(
            payment_month,
            func.count(distinct(Payment.id)).label("payment_count"),
            func.coalesce(func.sum(Payment.sum), 0).label("actual_collection_sum"),
        )
        .where(func.extract("year", Payment.payment_date) == year)
        .group_by(payment_month)
    )


async def get_plans_data(year: int) -> Select:
    """
    Asynchronously retrieves plan data grouped by month for a given year.

    This function constructs and returns an SQLAlchemy select statement that fetches the following data from the Plan table:
    - The period (month_year)
    - The maximum sum for category_id 3 (plan_issuance_sum), with NULL values replaced by 0
    - The maximum sum for category_id 4 (plan_collection_sum), with NULL values replaced by 0
    - The month of the period (period_month)

    The results are filtered by the provided year and grouped by the period.

    Args:
        year (int): The year for which to retrieve plan data.

    Returns:
        Select: An SQLAlchemy select statement that can be executed to fetch the data.
    """

    period_month = func.extract("month", Plan.period).label("period_month")

    return (
        select(
            Plan.period.label("month_year"),
            func.max(case((Plan.category_id == 3, Plan.sum), else_=0)).label(
                "plan_issuance_sum"
            ),
            func.max(case((Plan.category_id == 4, Plan.sum), else_=0)).label(
                "plan_collection_sum"
            ),
            period_month,
        )
        .where(func.extract("year", Plan.period) == year)
        .group_by(Plan.period)
    )


async def get_plans_performance_orm(db: AsyncSession, check_date: date) -> List[Tuple]:
    """
    Asynchronously retrieves plan performance data up to a given date.

    This function constructs and executes an SQLAlchemy query to fetch plan performance data, including actual sums
    calculated from Credits and Payments tables, up to the specified check date.

    Args:
        db (AsyncSession): The asynchronous database session.
        check_date (date): The date up to which plan performance is calculated.

    Returns:
        List[Tuple]: A list of result rows, each row represented as a tuple.
    """

    credits_sum_subquery = (
        select(func.sum(Credit.body))
        .where(Credit.issuance_date.between(Plan.period, check_date))
        .scalar_subquery()
    )

    payments_sum_subquery = (
        select(func.sum(Payment.sum))
        .where(Payment.payment_date.between(Plan.period, check_date))
        .scalar_subquery()
    )

    actual_sum_case = case(
        (Dictionary.id == 3, credits_sum_subquery),
        (Dictionary.id == 4, payments_sum_subquery),
        else_=0,
    )

    query = (
        select(
            Plan.period.label("month"),
            Dictionary.name.label("category"),
            Plan.sum.label("plan_sum"),
            func.coalesce(actual_sum_case, 0).label("actual_sum"),
        )
        .join(Dictionary, Plan.category_id == Dictionary.id)
        .where(Plan.period <= check_date)
    )

    result = await db.execute(query)
    return result.fetchall()


async def get_credits_with_payments_orm(db: AsyncSession, user_id: int) -> List[Tuple]:
    """
    Asynchronously retrieves credit information with associated payment details for a given user.

    This function constructs and executes an SQLAlchemy query to fetch credit data, including payment sums and
    overdue days, for a specific user.

    Args:
        db (AsyncSession): The asynchronous database session.
        user_id (int): The ID of the user for whom to retrieve credit information.

    Returns:
        List[Tuple]: A list of result rows, each row represented as a tuple.
    """
    dialect = db.bind.dialect.name
    overdue_days_case = case(
        (
            (Credit.actual_return_date.is_(None))
            & (Credit.return_date < current_date(dialect)),
            date_diff(current_date(dialect), Credit.return_date, dialect),
        ),
        else_=null(),
    )

    query = (
        select(
            Credit.id.label("credit_id"),
            Credit.issuance_date,
            (Credit.actual_return_date.is_not(None)).label("is_closed"),
            Credit.actual_return_date,
            Credit.return_date,
            Credit.body,
            Credit.percent,
            func.sum(Payment.sum).label("total_payments"),
            func.sum(case((Payment.type_id == 1, Payment.sum), else_=0)).label(
                "body_payments"
            ),
            func.sum(case((Payment.type_id == 2, Payment.sum), else_=0)).label(
                "percent_payments"
            ),
            overdue_days_case.label("overdue_days"),
        )
        .outerjoin(Payment, Credit.id == Payment.credit_id)
        .where(Credit.user_id == user_id)
        .group_by(Credit.id)
    )

    result = await db.execute(query)
    return result.fetchall()
