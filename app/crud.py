import logging
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql import text
from datetime import date
from models import Plan
import pandas as pd

logger = logging.getLogger(__name__)


async def get_user_credits(db: AsyncSession, user_id: int):
    query = text(
        """
        SELECT 
            c.id AS credit_id,
            c.issuance_date,
            CASE WHEN c.actual_return_date IS NOT NULL THEN 1 ELSE 0 END AS is_closed,
            c.actual_return_date,
            c.return_date,
            c.body,
            c.percent,
            COALESCE(SUM(p.sum), 0) AS total_payments,
            COALESCE(SUM(CASE WHEN p.type_id = 1 THEN p.sum ELSE 0 END), 0) AS body_payments,
            COALESCE(SUM(CASE WHEN p.type_id = 2 THEN p.sum ELSE 0 END), 0) AS percent_payments,
            CASE 
                WHEN c.actual_return_date IS NULL AND c.return_date < CURDATE() 
                THEN DATEDIFF(CURDATE(), c.return_date) 
                ELSE 0 
            END AS overdue_days
        FROM Credits c
        LEFT JOIN Payments p ON c.id = p.credit_id
        WHERE c.user_id = :user_id
        GROUP BY c.id
    """
    )
    result = await db.execute(query, {"user_id": user_id})
    rows = result.fetchall()
    return [
        {
            "credit_id": row[0],
            "issuance_date": row[1],
            "is_closed": bool(row[2]),
            "actual_return_date": row[3],
            "return_date": row[4],
            "overdue_days": row[10] if not row[2] else None,
            "body": row[5],
            "percent": row[6],
            "total_payments": row[7] if row[2] else None,
            "body_payments": row[8] if not row[2] else None,
            "percent_payments": row[9] if not row[2] else None,
        }
        for row in rows
    ]


async def check_existing_plan(db: AsyncSession, period: date, category_id: int) -> bool:
    query = text(
        "SELECT COUNT(*) FROM Plans WHERE period = :period AND category_id = :category_id"
    )
    result = await db.execute(query, {"period": period, "category_id": category_id})
    count = result.scalar()
    return count > 0


async def get_category_id(db: AsyncSession, category_name: str) -> int:
    query = text("SELECT id FROM Dictionary WHERE name = :name")
    result = await db.execute(query, {"name": category_name})
    category_id = result.scalar()
    if category_id is None:
        raise ValueError(f"Категория '{category_name}' не найдена в таблице Dictionary")
    return category_id


async def insert_plans(db: AsyncSession, df: pd.DataFrame):
    for _, row in df.iterrows():
        period = pd.to_datetime(row["month"], format="%Y-%m-%d").date()
        if period.day != 1:
            raise ValueError(f"Месяц плана '{period}' должен быть первым числом месяца")

        category_id = await get_category_id(db, row["category_name"])

        # Проверка на дубликат
        if await check_existing_plan(db, period, category_id):
            raise ValueError(
                f"План для месяца {period} и категории {row['category_name']} уже существует"
            )

        plan = Plan(period=period, sum=row["sum"], category_id=category_id)
        db.add(plan)
    logger.info("All plans prepared for insertion")


async def get_plans_performance(db: AsyncSession, check_date: date):
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
    return [
        {
            "month": row[0],
            "category": row[1],
            "plan_sum": row[2],
            "actual_sum": float(row[3]),
            "performance_percent": (
                round(float(row[3]) / row[2] * 100, 2) if row[2] > 0 else 0
            ),
        }
        for row in rows
    ]


logger = logging.getLogger(__name__)


async def get_year_performance(db: AsyncSession, year: int):
    # Получение общей суммы выдач за год
    total_issuance_result = await db.execute(
        text(
            "SELECT COALESCE(SUM(body), 0) FROM Credits WHERE YEAR(issuance_date) = :year"
        ),
        {"year": year},
    )
    total_issuance = float(total_issuance_result.scalar() or 0)  # Приведение к float

    # Получение общей суммы сборов за год
    total_collection_result = await db.execute(
        text(
            "SELECT COALESCE(SUM(sum), 0) FROM Payments WHERE YEAR(payment_date) = :year"
        ),
        {"year": year},
    )
    total_collection = float(
        total_collection_result.scalar() or 0
    )  # Приведение к float

    # Оптимизированный запрос с JOIN
    query = text(
        """
        SELECT 
            p.period AS month_year,
            COUNT(DISTINCT c.id) AS issuance_count,
            MAX(CASE WHEN p.category_id = 3 THEN p.sum ELSE 0 END) AS plan_issuance_sum,
            COALESCE(SUM(c.body), 0) AS actual_issuance_sum,
            COUNT(DISTINCT pm.id) AS payment_count,
            MAX(CASE WHEN p.category_id = 4 THEN p.sum ELSE 0 END) AS plan_collection_sum,
            COALESCE(SUM(pm.sum), 0) AS actual_collection_sum
        FROM Plans p
        LEFT JOIN Credits c ON YEAR(c.issuance_date) = :year AND MONTH(c.issuance_date) = MONTH(p.period)
        LEFT JOIN Payments pm ON YEAR(pm.payment_date) = :year AND MONTH(pm.payment_date) = MONTH(p.period)
        WHERE YEAR(p.period) = :year
        GROUP BY p.period
        """
    )
    try:
        logger.info(f"Fetching year performance for year: {year}")
        result = await db.execute(query, {"year": year})
        rows = result.fetchall()

        if not rows:
            logger.info(f"No performance data found for year: {year}")
            return []

        performance_list = []
        for row in rows:
            plan_issuance_sum = float(row[2] or 0)
            actual_issuance_sum = float(row[3] or 0)
            plan_collection_sum = float(row[5] or 0)
            actual_collection_sum = float(row[6] or 0)

            performance_list.append(
                {
                    "month_year": row[0].strftime("%Y-%m"),
                    "issuance_count": row[1],
                    "plan_issuance_sum": plan_issuance_sum,
                    "actual_issuance_sum": actual_issuance_sum,
                    "issuance_performance_percent": (
                        round(actual_issuance_sum / plan_issuance_sum * 100, 2)
                        if plan_issuance_sum > 0
                        else 0
                    ),
                    "payment_count": row[4],
                    "plan_collection_sum": plan_collection_sum,
                    "actual_collection_sum": actual_collection_sum,
                    "collection_performance_percent": (
                        round(actual_collection_sum / plan_collection_sum * 100, 2)
                        if plan_collection_sum > 0
                        else 0
                    ),
                    "issuance_percent_of_year": (
                        round(actual_issuance_sum / total_issuance * 100, 2)
                        if total_issuance > 0
                        else 0
                    ),
                    "collection_percent_of_year": (
                        round(actual_collection_sum / total_collection * 100, 2)
                        if total_collection > 0
                        else 0
                    ),
                }
            )

        logger.info(f"Year performance fetched: {len(performance_list)} rows")
        return performance_list
    except Exception as e:
        logger.error(f"Error in get_year_performance: {str(e)}")
        raise
