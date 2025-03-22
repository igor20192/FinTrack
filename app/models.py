from sqlalchemy import Column, Index, Integer, String, Date, Float, ForeignKey
from database import Base


class User(Base):
    """
    Represents a user in the system.

    Attributes:
        id (int): The unique identifier of the user.
        login (str): The login username of the user (unique).
        registration_date (Date): The date when the user registered.
    """

    __tablename__ = "Users"
    id = Column(Integer, primary_key=True)
    login = Column(String(50), unique=True, nullable=False)
    registration_date = Column(Date, nullable=False)


class Credit(Base):
    """
    Represents a credit record.

    Attributes:
        id (int): The unique identifier of the credit.
        user_id (int): The ID of the user who received the credit (foreign key to Users).
        issuance_date (Date): The date when the credit was issued.
        return_date (Date): The planned return date of the credit.
        actual_return_date (Date, optional): The actual return date of the credit (if returned).
        body (int): The principal amount of the credit.
        percent (float): The interest rate of the credit.
    """

    __tablename__ = "Credits"
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("Users.id"), nullable=False)
    issuance_date = Column(Date, nullable=False)
    return_date = Column(Date, nullable=False)
    actual_return_date = Column(Date, nullable=True)
    body = Column(Integer, nullable=False)
    percent = Column(Float, nullable=False)

    __table_args__ = (
        Index("idx_credits_issuance_date", "issuance_date"),
        Index("idx_credits_user_id", "user_id"),
    )


class Dictionary(Base):
    """
    Represents a dictionary entry.

    Attributes:
        id (int): The unique identifier of the dictionary entry.
        name (str): The name of the dictionary entry.
    """

    __tablename__ = "Dictionary"
    id = Column(Integer, primary_key=True)
    name = Column(String(50), nullable=False)

    __table_args__ = (Index("idx_dictionary_name", "name"),)


class Plan(Base):
    """
    Represents a plan record.

    Attributes:
        id (int): The unique identifier of the plan.
        period (Date): The period of the plan.
        sum (int): The planned sum.
        category_id (int): The category ID of the plan (foreign key to Dictionary).
    """

    __tablename__ = "Plans"
    id = Column(Integer, primary_key=True)
    period = Column(Date, nullable=False)
    sum = Column(Integer, nullable=False)
    category_id = Column(Integer, ForeignKey("Dictionary.id"), nullable=False)

    __table_args__ = (
        Index("idx_plans_period", "period"),
        Index("idx_plans_category_id", "category_id"),
    )


class Payment(Base):
    """
    Represents a payment record.

    Attributes:
        id (int): The unique identifier of the payment.
        sum (float): The payment amount.
        payment_date (Date): The date of the payment.
        credit_id (int): The ID of the credit associated with the payment (foreign key to Credits).
        type_id (int): The type ID of the payment (foreign key to Dictionary).
    """

    __tablename__ = "Payments"
    id = Column(Integer, primary_key=True)
    sum = Column(Float, nullable=False)
    payment_date = Column(Date, nullable=False)
    credit_id = Column(Integer, ForeignKey("Credits.id"), nullable=False)
    type_id = Column(Integer, ForeignKey("Dictionary.id"), nullable=False)

    __table_args__ = (
        Index("idx_payments_payment_date", "payment_date"),
        Index("idx_payments_credit_id", "credit_id"),
    )
