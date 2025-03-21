from sqlalchemy import Column, Index, Integer, String, Date, Float, ForeignKey
from database import Base


class User(Base):
    __tablename__ = "Users"
    id = Column(Integer, primary_key=True)
    login = Column(String(50), unique=True, nullable=False)
    registration_date = Column(Date, nullable=False)


class Credit(Base):
    __tablename__ = "Credits"
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("Users.id"), nullable=False)
    issuance_date = Column(Date, nullable=False)
    return_date = Column(Date, nullable=False)
    actual_return_date = Column(Date, nullable=True)
    body = Column(Integer, nullable=False)
    percent = Column(Float, nullable=False)

    __table_args__ = (
        Index("idx_credits_issuance_date", issuance_date),
        Index("idx_credits_user_id", user_id),  
    )


class Dictionary(Base):
    __tablename__ = "Dictionary"
    id = Column(Integer, primary_key=True)
    name = Column(String(50), nullable=False)

    __table_args__ = (
        Index("idx_dictionary_name", name),  
    )


class Plan(Base):
    __tablename__ = "Plans"
    id = Column(Integer, primary_key=True)
    period = Column(Date, nullable=False)
    sum = Column(Integer, nullable=False)
    category_id = Column(Integer, ForeignKey("Dictionary.id"), nullable=False)

    __table_args__ = (
        Index("idx_plans_period", period),
        Index("idx_plans_category_id", category_id),
    )


class Payment(Base):
    __tablename__ = "Payments"
    id = Column(Integer, primary_key=True)
    sum = Column(Float, nullable=False)
    payment_date = Column(Date, nullable=False)
    credit_id = Column(Integer, ForeignKey("Credits.id"), nullable=False)
    type_id = Column(Integer, ForeignKey("Dictionary.id"), nullable=False)

    __table_args__ = (
        Index("idx_payments_payment_date", payment_date),
        Index("idx_payments_credit_id", credit_id),  
    )
