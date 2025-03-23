# app/queries.py
from datetime import date
from sqlalchemy import null, select, func, case
from sqlalchemy.sql import distinct
from sqlalchemy.ext.asyncio import AsyncSession
from models import Credit, Dictionary, Payment, Plan


# ORM queries for get_year_performance
async def get_credits_data(year: int):
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
        sqlalchemy.sql.selectable.Select: An SQLAlchemy select statement that can be executed to fetch the data.
    """

    return (
        select(
            func.extract("month", Credit.issuance_date).label("issuance_month"),
            func.count(distinct(Credit.id)).label("issuance_count"),
            func.coalesce(func.sum(Credit.body), 0).label("actual_issuance_sum"),
        )
        .where(func.extract("year", Credit.issuance_date) == year)
        .group_by(func.extract("month", Credit.issuance_date))
    )


async def get_payments_data(year: int):
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
        sqlalchemy.sql.selectable.Select: An SQLAlchemy select statement that can be executed to fetch the data.
    """
    return (
        select(
            func.extract("month", Payment.payment_date).label("payment_month"),
            func.count(distinct(Payment.id)).label("payment_count"),
            func.coalesce(func.sum(Payment.sum), 0).label("actual_collection_sum"),
        )
        .where(func.extract("year", Payment.payment_date) == year)
        .group_by(func.extract("month", Payment.payment_date))
    )


async def get_plans_data(year: int):
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
        sqlalchemy.sql.selectable.Select: An SQLAlchemy select statement that can be executed to fetch the data.
    """
    return (
        select(
            Plan.period.label("month_year"),
            func.max(case((Plan.category_id == 3, Plan.sum), else_=0)).label(
                "plan_issuance_sum"
            ),
            func.max(case((Plan.category_id == 4, Plan.sum), else_=0)).label(
                "plan_collection_sum"
            ),
            func.extract("month", Plan.period).label("period_month"),
        )
        .where(func.extract("year", Plan.period) == year)
        .group_by(Plan.period)
    )


async def get_plans_performance_orm(db: AsyncSession, check_date: date):
    """
    Asynchronously retrieves plan performance data up to a given date.

    This function constructs and executes an SQLAlchemy query to fetch plan performance data, including actual sums
    calculated from Credits and Payments tables, up to the specified check date.

    Args:
        db (AsyncSession): The asynchronous database session.
        check_date (date): The date up to which plan performance is calculated.

    Returns:
        list: A list of result rows, each row represented as a tuple.
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

    query = (
        select(
            Plan.period.label("month"),
            Dictionary.name.label("category"),
            Plan.sum.label("plan_sum"),
            func.coalesce(
                case(
                    (Dictionary.id == 3, credits_sum_subquery),
                    (Dictionary.id == 4, payments_sum_subquery),
                    else_=0,
                ),
                0,
            ).label("actual_sum"),
        )
        .join(Dictionary, Plan.category_id == Dictionary.id)
        .where(Plan.period <= check_date)
    )

    result = await db.execute(query)
    return result.fetchall()


async def get_credits_with_payments_orm(db: AsyncSession, user_id: int):
    """
    Asynchronously retrieves credit information with associated payment details for a given user.

    This function constructs and executes an SQLAlchemy query to fetch credit data, including payment sums and
    overdue days, for a specific user.

    Args:
        db (AsyncSession): The asynchronous database session.
        user_id (int): The ID of the user for whom to retrieve credit information.

    Returns:
        list: A list of result rows, each row represented as a tuple.
    """
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
            case(
                (
                    (Credit.actual_return_date.is_(None))
                    & (Credit.return_date < func.curdate()),
                    func.datediff(func.curdate(), Credit.return_date),
                ),
                else_=null(),
            ).label("overdue_days"),
        )
        .outerjoin(Payment, Credit.id == Payment.credit_id)
        .where(Credit.user_id == user_id)
        .group_by(Credit.id)
    )

    result = await db.execute(query)
    return result.fetchall()
