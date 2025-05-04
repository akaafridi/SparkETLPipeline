"""
Great Expectations configuration and utility functions.
"""
import os
import logging
import great_expectations as ge
from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.data_context import BaseDataContext
from great_expectations.data_context.types.base import (
    DataContextConfig, 
    FilesystemStoreBackendDefaults
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def initialize_data_context():
    """
    Initialize a Great Expectations data context with default configuration.
    
    Returns:
        BaseDataContext: Configured Great Expectations data context
    """
    logger.info("Initializing Great Expectations context")
    
    # Create a simple project structure
    project_root_dir = os.getcwd()
    ge_root_dir = os.path.join(project_root_dir, "great_expectations")
    
    # Create GE directory if it doesn't exist
    os.makedirs(ge_root_dir, exist_ok=True)
    
    # Configure the data context
    data_context_config = DataContextConfig(
        store_backend_defaults=FilesystemStoreBackendDefaults(
            root_directory=ge_root_dir
        ),
        expectations_store_name="expectations_store",
        validations_store_name="validations_store",
        evaluation_parameter_store_name="evaluation_parameter_store",
        checkpoint_store_name="checkpoint_store",
        data_docs_sites={
            "local_site": {
                "class_name": "SiteBuilder",
                "store_backend": {
                    "class_name": "TupleFilesystemStoreBackend",
                    "base_directory": os.path.join(ge_root_dir, "uncommitted", "data_docs", "local_site"),
                },
                "site_index_builder": {
                    "class_name": "DefaultSiteIndexBuilder",
                },
            }
        },
    )
    
    context = BaseDataContext(project_config=data_context_config)
    logger.info("Great Expectations context initialized")
    return context

def create_expectation_suite(context, suite_name="parquet_output_suite"):
    """
    Create or get an expectation suite.
    
    Args:
        context (BaseDataContext): Great Expectations context
        suite_name (str, optional): Name of the expectation suite. Defaults to "parquet_output_suite".
    
    Returns:
        ExpectationSuite: The created or retrieved expectation suite
    """
    logger.info(f"Creating expectation suite: {suite_name}")
    
    try:
        # Try to get existing suite
        suite = context.get_expectation_suite(suite_name)
        logger.info(f"Using existing expectation suite: {suite_name}")
    except:
        # Create new suite if it doesn't exist
        suite = context.create_expectation_suite(suite_name)
        logger.info(f"Created new expectation suite: {suite_name}")
    
    return suite

def add_expectations_to_suite(suite):
    """
    Add expectations to the suite to validate Parquet output.
    
    Args:
        suite: Great Expectations suite object
    
    Returns:
        The updated suite
    """
    logger.info("Adding expectations to suite")
    
    # Add expectation to check for required columns
    suite.add_expectation(
        ExpectationConfiguration(
            expectation_type="expect_table_columns_to_match_ordered_list",
            kwargs={
                "column_list": ["ImageID", "LabelName", "Confidence"]
            }
        )
    )
    
    # Add expectation to check for non-null ImageID values
    suite.add_expectation(
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_not_be_null",
            kwargs={
                "column": "ImageID"
            }
        )
    )
    
    # Add expectation to check for row count
    suite.add_expectation(
        ExpectationConfiguration(
            expectation_type="expect_table_row_count_to_be_between",
            kwargs={
                "min_value": 1
            }
        )
    )
    
    # Add column type expectations
    suite.add_expectation(
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_of_type",
            kwargs={
                "column": "ImageID",
                "type_": "str"
            }
        )
    )
    
    suite.add_expectation(
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_of_type",
            kwargs={
                "column": "LabelName",
                "type_": "str"
            }
        )
    )
    
    suite.add_expectation(
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_of_type",
            kwargs={
                "column": "Confidence",
                "type_": "str"  # CSV files load numeric values as strings by default
            }
        )
    )
    
    logger.info("Expectations added to suite")
    return suite

def setup_default_expectations_suite():
    """
    Set up the default expectations suite for the ETL pipeline.
    
    Returns:
        tuple: (context, suite_name) for validation
    """
    logger.info("Setting up default expectations suite")
    
    # Initialize data context
    context = initialize_data_context()
    
    # Create or get suite
    suite_name = "parquet_output_suite"
    suite = create_expectation_suite(context, suite_name)
    
    # Add expectations to the suite
    suite = add_expectations_to_suite(suite)
    
    # Save the suite
    context.save_expectation_suite(suite)
    logger.info(f"Expectation suite '{suite_name}' saved")
    
    return context, suite_name

def validate_parquet_data(parquet_path, spark_session, context=None, suite_name=None):
    """
    Validate the Parquet data using Great Expectations.
    
    Args:
        parquet_path (str): Path to the Parquet files
        spark_session: Spark session to use for reading data
        context (BaseDataContext, optional): GE context. If None, a new one is created.
        suite_name (str, optional): Name of the suite to use. If None, default is used.
    
    Returns:
        ValidationResult: The result of the validation
    """
    logger.info(f"Validating Parquet data at: {parquet_path}")
    
    # Initialize context and suite if not provided
    if context is None or suite_name is None:
        context, suite_name = setup_default_expectations_suite()
    
    # Load Parquet data as Spark DataFrame
    df = spark_session.read.parquet(parquet_path)
    
    # Convert to GE DataFrame
    ge_df = ge.dataset.SparkDFDataset(df)
    
    # Create batch request
    batch_request = RuntimeBatchRequest(
        datasource_name="my_spark_datasource",
        data_connector_name="default_runtime_data_connector_name",
        data_asset_name="parquet_data",
        batch_identifiers={"batch_id": "default_batch_id"},
        runtime_parameters={"batch_data": ge_df}
    )
    
    # Create validator
    validator = context.get_validator(
        batch_request=batch_request,
        expectation_suite_name=suite_name
    )
    
    # Run validation
    validation_result = validator.validate()
    logger.info(f"Validation completed with success: {validation_result.success}")
    
    # Save validation result and build data docs
    context.build_data_docs()
    logger.info("Data docs generated")
    
    return validation_result

if __name__ == "__main__":
    # When run directly, this script will set up the default expectations suite
    setup_default_expectations_suite()
