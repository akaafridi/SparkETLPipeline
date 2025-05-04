"""
Prefect flow to orchestrate the ETL pipeline.
"""
import os
import logging
import sys
from datetime import datetime

# Ensure the project root is in the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from prefect import flow, task
from prefect.task_runners import SequentialTaskRunner

from etl.ingest import ingest
from etl.expectations.expectations import (
    setup_default_expectations_suite,
    validate_parquet_data
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@task(name="Run ETL Process")
def run_etl(input_path=None, output_path=None):
    """
    Task to run the ETL process.
    
    Args:
        input_path (str, optional): Path to the input CSV file
        output_path (str, optional): Path to write Parquet output
    
    Returns:
        tuple: (spark_session, output_path) for validation
    """
    logger.info("Starting ETL process")
    return ingest(input_path, output_path)

@task(name="Setup Expectations")
def setup_expectations():
    """
    Task to set up Great Expectations.
    
    Returns:
        tuple: (context, suite_name) for validation
    """
    logger.info("Setting up Great Expectations")
    return setup_default_expectations_suite()

@task(name="Validate Data")
def validate_data(etl_result, ge_setup):
    """
    Task to validate the ETL output using Great Expectations.
    
    Args:
        etl_result (tuple): (spark_session, output_path) from ETL task
        ge_setup (tuple): (context, suite_name) from setup task
    
    Returns:
        bool: True if validation passes, raises exception otherwise
    """
    logger.info("Starting data validation")
    spark_session, output_path = etl_result
    context, suite_name = ge_setup
    
    validation_result = validate_parquet_data(
        output_path,
        spark_session,
        context,
        suite_name
    )
    
    if not validation_result.success:
        logger.error("Data validation failed")
        raise Exception("Data validation failed. Check Great Expectations data docs for details.")
    
    logger.info("Data validation passed")
    return True

@flow(
    name="ETL Pipeline",
    description="End-to-end ETL pipeline with data validation",
    task_runner=SequentialTaskRunner(),
)
def etl_pipeline(input_path=None, output_path=None):
    """
    Prefect flow to orchestrate the ETL pipeline.
    
    Args:
        input_path (str, optional): Path to input CSV file
        output_path (str, optional): Path to write Parquet output
    
    Returns:
        bool: True if the pipeline runs successfully
    """
    # Set default paths if not provided
    if input_path is None:
        input_path = "data/labels.csv"
    if output_path is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = f"data/output/parquet_{timestamp}"
    
    logger.info(f"Starting ETL pipeline at {datetime.now()}")
    logger.info(f"Input path: {input_path}")
    logger.info(f"Output path: {output_path}")
    
    # Run ETL task
    etl_result = run_etl(input_path, output_path)
    
    # Setup expectations
    ge_setup = setup_expectations()
    
    # Validate data
    validation_success = validate_data(etl_result, ge_setup)
    
    # Clean up Spark session
    spark_session, _ = etl_result
    spark_session.stop()
    
    logger.info(f"ETL pipeline completed at {datetime.now()}")
    return validation_success

if __name__ == "__main__":
    # When run directly, execute the ETL pipeline
    etl_pipeline()
