import pandas as pd
import os
import logging

# Setting up logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Folder for saving files
output_dir = "Plans"
os.makedirs(output_dir, exist_ok=True)


def create_plans_xlsx():
    """Creates plans.xlsx for /plans_insert."""
    plans_xlsx_data = {
        "month": ["2023-03-01", "2023-03-01", "2023-04-01", "2023-04-01"],
        "category_name": ["видача", "збір", "видача", "збір"],
        "sum": [25000, 6000, 13000, 17000],
    }
    plans_xlsx_df = pd.DataFrame(plans_xlsx_data)
    plans_xlsx_df.to_excel(
        os.path.join(output_dir, "plans.xlsx"), index=False, engine="openpyxl"
    )
    logger.info("plans.xlsx created successfully.")


def create_plans_wrong_date():
    """Creates plans_wrong_date.xlsx with incorrect date."""
    plans_wrong_date = {
        "month": ["2020-03-15", "2020-03-01"],
        "category_name": ["видача", "збір"],
        "sum": [25000, 6000],
    }
    pd.DataFrame(plans_wrong_date).to_excel(
        os.path.join(output_dir, "plans_wrong_date.xlsx"),
        index=False,
        engine="openpyxl",
    )
    logger.info("plans_wrong_date.xlsx created successfully.")


def create_plans_empty_sum():
    """Creates plans_empty_sum.xlsx with empty sum."""
    plans_empty_sum = {
        "month": ["2020-03-01", "2020-03-01"],
        "category_name": ["видача", "збір"],
        "sum": [None, 6000],
    }
    pd.DataFrame(plans_empty_sum).to_excel(
        os.path.join(output_dir, "plans_empty_sum.xlsx"), index=False, engine="openpyxl"
    )
    logger.info("plans_empty_sum.xlsx created successfully.")


def create_plans_wrong_structure():
    """Creates plans_wrong_structure.xlsx with incorrect structure."""
    plans_wrong_structure = {
        "wrong_column": ["2020-03-01"],
        "category_name": ["видача"],
        "sum": [25000],
    }
    pd.DataFrame(plans_wrong_structure).to_excel(
        os.path.join(output_dir, "plans_wrong_structure.xlsx"),
        index=False,
        engine="openpyxl",
    )
    logger.info("plans_wrong_structure.xlsx created successfully.")


def create_all_test_files():
    """Creates all test files for /plans_insert."""
    create_plans_xlsx()
    create_plans_wrong_date()
    create_plans_empty_sum()
    create_plans_wrong_structure()
    logger.info("All test files created successfully!")


if __name__ == "__main__":
    create_all_test_files()
