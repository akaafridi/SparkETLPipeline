"""
Simple ETL runner without Prefect dependency.
"""
import os
import sys
import logging
from datetime import datetime

# Ensure the project root is in the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

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

def run_etl_pipeline(input_path=None, output_path=None):
    """
    Run the ETL pipeline without Prefect.
    
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
    
    try:
        # Run ETL
        logger.info("Starting ETL process")
        spark_session, output_path = ingest(input_path, output_path)
        
        # Setup expectations
        logger.info("Setting up Great Expectations")
        context, suite_name = setup_default_expectations_suite()
        
        # Validate data
        logger.info("Starting data validation")
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
        
        # Clean up Spark session
        spark_session.stop()
        
        logger.info(f"ETL pipeline completed at {datetime.now()}")
        return True
    
    except Exception as e:
        logger.error(f"Error in ETL pipeline: {str(e)}")
        return False

if __name__ == "__main__":
    # When run directly, execute the ETL pipeline
    run_etl_pipeline()