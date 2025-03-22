from pydantic import BaseModel
from datetime import date
from typing import Optional


class CreditResponse(BaseModel):
    """
    Represents the response model for credit information.
    """

    credit_id: int
    issuance_date: date
    is_closed: bool
    actual_return_date: Optional[date] = None
    return_date: Optional[date] = None
    overdue_days: Optional[int] = None
    body: int
    percent: float
    total_payments: Optional[float] = None
    body_payments: Optional[float] = None
    percent_payments: Optional[float] = None

    class Config:
        from_attributes = True


class PlanPerformanceResponse(BaseModel):
    """
    Represents the response model for plan performance.
    """

    month: date
    category: str
    plan_sum: int
    actual_sum: float
    performance_percent: float


class YearPerformanceResponse(BaseModel):
    """
    Represents the response model for yearly performance data.
    """

    month_year: str
    issuance_count: int
    plan_issuance_sum: int
    actual_issuance_sum: float
    issuance_performance_percent: float
    payment_count: int
    plan_collection_sum: int
    actual_collection_sum: float
    collection_performance_percent: float
    issuance_percent_of_year: float
    collection_percent_of_year: float
