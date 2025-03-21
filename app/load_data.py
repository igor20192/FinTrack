import pandas as pd
from sqlalchemy.ext.asyncio import AsyncSession
from database import async_session, Base, engine
from models import User, Credit, Dictionary, Plan, Payment
import asyncio
import logging

# Setting up logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


async def load_users(db: AsyncSession):
    """Loads users from users.csv into the database."""
    try:
        df = pd.read_csv("users.csv", sep="\t")
        logger.info(f"Loaded users.csv with columns: {list(df.columns)}")
        for _, row in df.iterrows():
            db.add(
                User(
                    id=row["id"],
                    login=row["login"],
                    registration_date=pd.to_datetime(
                        row["registration_date"], format="%d.%m.%Y"
                    ).date(),
                )
            )
        await db.commit()
        logger.info("Users loaded successfully")
    except Exception as e:
        logger.error(f"Error loading users: {e}")
        raise


async def load_dictionary(db: AsyncSession):
    """Loads dictionary data from dictionary.csv into the database."""
    try:
        df = pd.read_csv("dictionary.csv", sep="\t")
        logger.info(f"Loaded dictionary.csv with columns: {list(df.columns)}")
        for _, row in df.iterrows():
            db.add(Dictionary(id=row["id"], name=row["name"]))
        await db.commit()
        logger.info("Dictionary loaded successfully")
    except Exception as e:
        logger.error(f"Error loading dictionary: {e}")
        raise


async def load_credits(db: AsyncSession):
    """Loads credit data from credits.csv into the database."""
    try:
        df = pd.read_csv("credits.csv", sep="\t")
        logger.info(f"Loaded credits.csv with columns: {list(df.columns)}")
        for _, row in df.iterrows():
            db.add(
                Credit(
                    id=row["id"],
                    user_id=row["user_id"],
                    issuance_date=pd.to_datetime(
                        row["issuance_date"], format="%d.%m.%Y"
                    ).date(),
                    return_date=pd.to_datetime(
                        row["return_date"], format="%d.%m.%Y"
                    ).date(),
                    actual_return_date=(
                        pd.to_datetime(
                            row["actual_return_date"], format="%d.%m.%Y"
                        ).date()
                        if pd.notna(row["actual_return_date"])
                        else None
                    ),
                    body=row["body"],
                    percent=row["percent"],
                )
            )
        await db.commit()
        logger.info("Credits loaded successfully")
    except Exception as e:
        logger.error(f"Error loading credits: {e}")
        raise


async def load_plans(db: AsyncSession):
    """Loads plan data from plans.csv into the database."""
    try:
        df = pd.read_csv("plans.csv", sep="\t")
        logger.info(f"Loaded plans.csv with columns: {list(df.columns)}")
        for _, row in df.iterrows():
            db.add(
                Plan(
                    id=row["id"],
                    period=pd.to_datetime(row["period"], format="%d.%m.%Y").date(),
                    sum=row["sum"],
                    category_id=row["category_id"],
                )
            )
        await db.commit()
        logger.info("Plans loaded successfully")
    except Exception as e:
        logger.error(f"Error loading plans: {e}")
        raise


async def load_payments(db: AsyncSession):
    """Loads payment data from payments.csv into the database."""
    try:
        df = pd.read_csv("payments.csv", sep="\t")
        logger.info(f"Loaded payments.csv with columns: {list(df.columns)}")
        for _, row in df.iterrows():
            db.add(
                Payment(
                    id=row["id"],
                    credit_id=row["credit_id"],
                    payment_date=pd.to_datetime(
                        row["payment_date"], format="%d.%m.%Y"
                    ).date(),
                    type_id=row["type_id"],
                    sum=row["sum"],
                )
            )
        await db.commit()
        logger.info("Payments loaded successfully")
    except Exception as e:
        logger.error(f"Error loading payments: {e}")
        raise


async def init_db():
    """Initializes the database by creating tables and loading data from CSV files."""
    try:
        async with engine.begin() as conn:
            await conn.run_sync(
                Base.metadata.drop_all
            )  # Remove tables before loading (optional)
            await conn.run_sync(Base.metadata.create_all)  # Creating tables again

        async with async_session() as db:
            await load_users(db)
            await load_dictionary(db)
            await load_credits(db)
            await load_plans(db)
            await load_payments(db)

        await engine.dispose()
        logger.info("Database initialization completed")
    except Exception as e:
        logger.error(f"Error during database initialization: {e}")
        raise


if __name__ == "__main__":
    asyncio.run(init_db())
