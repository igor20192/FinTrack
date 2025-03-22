import asyncio
import pandas as pd
from sqlalchemy.ext.asyncio import AsyncSession
from database import async_session, engine
from models import Base, User, Credit, Dictionary, Plan, Payment
import logging

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


async def init_db():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)
    logger.info("Database initialized")


async def load_users(db: AsyncSession, file_path: str):
    df = pd.read_csv(file_path, sep="\t")
    for _, row in df.iterrows():
        user = User(
            id=row["id"],
            login=row["login"],
            registration_date=pd.to_datetime(
                row["registration_date"], format="%d.%m.%Y"
            ).date(),
        )
        db.add(user)
    await db.commit()
    logger.info(f"Loaded {len(df)} users")


async def load_dictionary(db: AsyncSession, file_path: str):
    df = pd.read_csv(file_path, sep="\t")
    for _, row in df.iterrows():
        dictionary = Dictionary(id=row["id"], name=row["name"])
        db.add(dictionary)
    await db.commit()
    logger.info(f"Loaded {len(df)} dictionary entries")


async def load_credits(db: AsyncSession, file_path: str):
    df = pd.read_csv(file_path, sep="\t")
    for _, row in df.iterrows():
        credit = Credit(
            id=row["id"],
            user_id=row["user_id"],
            issuance_date=pd.to_datetime(
                row["issuance_date"], format="%d.%m.%Y"
            ).date(),
            return_date=pd.to_datetime(row["return_date"], format="%d.%m.%Y").date(),
            actual_return_date=(
                pd.to_datetime(row["actual_return_date"], format="%d.%m.%Y").date()
                if pd.notna(row["actual_return_date"])
                else None
            ),
            body=row["body"],
            percent=row["percent"],
        )
        db.add(credit)
    await db.commit()
    logger.info(f"Loaded {len(df)} credits")


async def load_plans(db: AsyncSession, file_path: str):
    df = pd.read_csv(file_path, sep="\t")
    for _, row in df.iterrows():
        plan = Plan(
            id=row["id"],
            period=pd.to_datetime(row["period"], format="%d.%m.%Y").date(),
            sum=row["sum"],
            category_id=row["category_id"],
        )
        db.add(plan)
    await db.commit()
    logger.info(f"Loaded {len(df)} plans")


async def load_payments(db: AsyncSession, file_path: str):
    df = pd.read_csv(file_path, sep="\t")
    for _, row in df.iterrows():
        payment = Payment(
            id=row["id"],
            sum=row["sum"],
            payment_date=pd.to_datetime(row["payment_date"], format="%d.%m.%Y").date(),
            credit_id=row["credit_id"],
            type_id=row["type_id"],
        )
        db.add(payment)
    await db.commit()
    logger.info(f"Loaded {len(df)} payments")


async def main():
    async with async_session() as db:
        await init_db()

        data_dir = "/home/igor/Projects/FinTrack"
        await load_users(db, f"{data_dir}/users.csv")
        await load_dictionary(db, f"{data_dir}/dictionary.csv")
        await load_credits(db, f"{data_dir}/credits.csv")
        await load_plans(db, f"{data_dir}/plans.csv")
        await load_payments(db, f"{data_dir}/payments.csv")


if __name__ == "__main__":
    asyncio.run(main())
