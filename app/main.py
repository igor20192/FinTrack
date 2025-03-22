import logging
from fastapi import FastAPI, Request, UploadFile, HTTPException, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from database import get_db, engine, Base
from schemas import CreditResponse, PlanPerformanceResponse, YearPerformanceResponse
from crud import (
    get_user_credits,
    insert_plans,
    get_plans_performance,
    get_year_performance,
)
import pandas as pd
from datetime import date
from contextlib import asynccontextmanager
from fastapi.middleware.cors import CORSMiddleware

# Setting up logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

app = FastAPI()


app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.middleware("http")
async def log_requests(request: Request, call_next):
    logger.info(f"Request: {request.method} {request.url}")
    response = await call_next(request)
    logger.info(f"Response status: {response.status_code}")
    return response


# Create tables (done synchronously at startup)


async def init_db():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()  # Create tables
    yield


# Endpoints
@app.get("/user_credits/{user_id}", response_model=list[CreditResponse])
async def user_credits(user_id: int, db: AsyncSession = Depends(get_db)):
    """
    Retrieves a list of user credits by their ID.

    Args:
        user_id (int): The unique identifier of the user.
        db (AsyncSession, optional): The asynchronous database session, passed via Depends.

    Returns:
        list[CreditResponse]: A list of user credits.

    Raises:
        HTTPException 404: If no credits are found for the specified user.
        HTTPException 500: In case of an error during the request execution.
    """
    logger.info(f"Fetching credits for user_id: {user_id}")
    try:
        credits = await get_user_credits(db, user_id)
        if not credits:
            raise HTTPException(
                status_code=404, detail="Credits not found for this user"
            )
        return credits
    except Exception as e:
        logger.error(f"Error fetching credits: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/plans_insert")
async def plans_insert(file: UploadFile, db: AsyncSession = Depends(get_db)):
    """
    Uploads and inserts financial plans from an Excel file into the database.

    Args:
        file (UploadFile): The uploaded Excel file containing financial plan data.
        db (AsyncSession, optional): The asynchronous database session, passed via Depends.

    Returns:
        dict: A message indicating successful insertion of plans.

    Raises:
        HTTPException 400: If the file structure is incorrect or contains missing values in the 'sum' column.
        HTTPException 500: In case of an error during data processing or database insertion.
    """
    logger.info(f"Received file: {file.filename}")
    try:
        # Read the Excel file
        df = pd.read_excel(file.file)
        expected_columns = ["month", "category_name", "sum"]
        if not all(col in df.columns for col in expected_columns):
            raise HTTPException(
                status_code=400,
                detail="Неверная структура файла: отсутствуют столбцы month, category_name или sum",
            )

        # Check for missing values in the 'sum' column
        if df["sum"].isna().any():
            raise HTTPException(
                status_code=400, detail="Столбец 'sum' содержит пустые значения"
            )

        # Validate and insert data
        await insert_plans(db, df)
        await db.commit()
        logger.info("Plans successfully inserted")
        return {"message": "Планы успешно загружены в базу данных"}
    except ValueError as e:
        await db.rollback()
        logger.error(f"Validation error: {str(e)}")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        await db.rollback()
        logger.error(f"Error inserting plans: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/plans_performance", response_model=list[PlanPerformanceResponse])
async def plans_performance(check_date: date, db: AsyncSession = Depends(get_db)):
    """
    Retrieves the performance of financial plans for a specific date.

    Args:
        check_date (date): The date for which the plan performance is requested.
        db (AsyncSession, optional): The asynchronous database session, passed via Depends.

    Returns:
        list[PlanPerformanceResponse]: A list of financial plan performance data.

    Raises:
        HTTPException 500: If an error occurs while fetching the data from the database.
    """
    logger.info(f"Fetching plans performance for date: {check_date}")
    try:
        plans = await get_plans_performance(db, check_date)
        return plans
    except Exception as e:
        logger.error(f"Error fetching plans performance: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/year_performance", response_model=list[YearPerformanceResponse])
async def year_performance(year: int, db: AsyncSession = Depends(get_db)):
    """
    Retrieves the financial performance for a specific year.

    Args:
        year (int): The year for which the performance data is requested.
        db (AsyncSession, optional): The asynchronous database session, passed via Depends.

    Returns:
        list[YearPerformanceResponse]: A list of financial performance data for the given year.

    Raises:
        HTTPException 500: If an error occurs while fetching the data from the database.
    """
    logger.info(f"Fetching year performance for year: {year}")
    try:
        performance = await get_year_performance(db, year)
        return performance
    except Exception as e:
        logger.error(f"Error fetching year performance: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
