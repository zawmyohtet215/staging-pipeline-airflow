from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.models import Variable
from hdbcli import dbapi
import psycopg2
import logging
import pandas as pd
import io
from datetime import datetime, timedelta
import calendar

## Optimized Version

# Define global variable for required columns
REQUIRED_COLUMNS = ["CustomerCode", "SellingPriceList", "SeriesName", "DocEntry", "DocNum", 
                    "LineNum", "PostingDate", "DueDate", "DocumentDate", "InvoiceType", 
                    "ItemCode", "Quantity", "Status", "Price", "LineTotal", "DocDiscount", "DiscountSum", 
                    "RowDiscountPercentage", "DocTotal", "WhsCode", "BPBranch", "Branch", "Department", 
                    "AgentCode", "AgentCommissionAmount", "Project", "Currency", "GProfit", "LDtotal", 
                    "GrossTotal", "StPrice", "StValue", "INMPrice", 
                    "TotalBasePrice(LPP)", "TotalGrossProfit(LPP)", "GLActC", "CogsActC"]

def connect_to_sap_hana():
    """Connect to SAP HANA using Airflow Variables"""
    host = Variable.get("SAP_HOST")
    port = int(Variable.get("SAP_PORT"))  
    user = Variable.get("SAP_USERNAME")
    password = Variable.get("SAP_PASSWORD")

    try:
        connection = dbapi.connect(address=host, port=port, user=user, password=password)
        return connection
    except dbapi.Error as e:
        logging.error(f"Error connecting to SAP HANA: {e}")
        raise

def connect_to_pgsql():
    """Connect to PostgreSQL using Airflow Variables"""
    host = Variable.get("PGSQL_HOST")
    port = int(Variable.get("PGSQL_PORT"))
    database = Variable.get("PGSQL_DATABASE")
    username = Variable.get("PGSQL_USERNAME")
    password = Variable.get("PGSQL_PASSWORD")
    
    try:
        connection = psycopg2.connect(
            database=database,
            host=host,
            port=port,
            user=username,
            password=password
        )
        return connection
    except psycopg2.Error as e:
        logging.error(f"PostgreSQL connection failed! Error: {e}")
        raise
        

def extract_fact_arcm():
    """
        Extracts AR Data - Current Month from SAP using the specified SQL query and saves the result as a temporary CSV file.
    """
    connection = connect_to_sap_hana()
    extract_f_arcm_gbs_query = Variable.get("EXTRACT_ARCM_GBS_QUERY")     # get arcm_gbs sql query
    
    try:
        df = pd.read_sql_query(extract_f_arcm_gbs_query, connection) # extract data as dataframe
        # Check if the DataFrame is empty
        if df.empty:
            logging.error("No data to load into PostgreSQL.")
            return

        # Transform data
        ## Get required columns
        df = df[REQUIRED_COLUMNS]

        df = df.sort_values(by="DocNum", ascending=True)

        # Create a composite key column to detect duplicated rows
        duplicated_count = df.duplicated(subset=['CustomerCode','SellingPriceList','SeriesName','DocEntry','DocNum',
                                                'LineNum','ItemCode']).sum()
        if duplicated_count > 0:
            logging.info("There are duplicated rows! Please check data manually.")
            return None
        
        logging.info("ARCM extraction completed successfully.")
        return df
    
    except Exception as e:
        logging.error(f"There is an error {e}", exc_info=True)
    finally:
        connection.close() # close sap connection
            
    
def extract_fact_arlm():
    """Extracts Last Month data from SAP using SQL query and saves the result as a CSV file."""

    connection = connect_to_sap_hana()
    extract_f_arlm_gbs_query = Variable.get("EXTRACT_ARLM_GBS_QUERY")
    try:
        df = pd.read_sql_query(extract_f_arlm_gbs_query, connection)
        # Transform data
        ## Get required columns
        df = df[REQUIRED_COLUMNS]

        df = df.sort_values(by="DocNum", ascending=True)

        # Create a composite key column to detect duplicated rows
        duplicated_count = df.duplicated(subset=['CustomerCode','SellingPriceList','SeriesName','DocEntry','DocNum',
                                                'LineNum','ItemCode']).sum()
        if duplicated_count > 0:
            logging.info("There are duplicated rows! Please check data manually.")
            return None
        
        logging.info("ARCM extraction completed successfully.")
        return df
    
    except Exception as e:
        logging.info(f"There is an error {e}", exc_info=True)
    finally:
        connection.close()


def load_fact_arcm(arcm_df):
    """Loads AR Data - CURRENT MONTH into pgsql using the COPY command with in-memory CSV."""
    try:
        connection = connect_to_pgsql()
        cursor = connection.cursor()
    
        # Delete and Update AR Data WHERE PostingDate = Current Month to update the whole month
        cursor.execute("""DELETE FROM "ar_gbs"."fact_ar" WHERE 
            "postingdate" = date_trunc('month', current_date)""")

        # Convert DataFrame to an in-memory CSV (string)
        output = io.StringIO()
        arcm_df.to_csv(output, index=False, header=True)
        output.seek(0)  # Go to the beginning of the string buffer

        # Prepare COPY command
        copy_query = """
        COPY "ar_gbs"."fact_ar" (
            CustomerCode, SellingPriceList, SeriesName, DocEntry, DocNum, LineNum, 
            PostingDate, DueDate, DocumentDate, InvoiceType, ItemCode, Quantity, 
            Status, Price, LineTotal, DocDiscount, DiscountSum, RowDiscountPercentage, 
            DocTotal, WhsCode, BPBranch, Branch, Department, AgentCode, 
            AgentCommissionAmount, Project, Currency, GProfit, LDtotal, GrossTotal, 
            StPrice, StValue, INMPrice, TotalBasePrice_LPP, TotalGrossProfit_LPP, 
            GLActC, CogsActC)
        FROM stdin WITH CSV HEADER DELIMITER ',';
        """
  
        cursor.copy_expert(sql=copy_query, file=output) # Execute COPY command
        connection.commit()

    except Exception as e:
        logging.error(f"Error occurred while loading data: {e}", exc_info=True)
        connection.rollback()
    finally:
        cursor.close()
        connection.close()
            
def load_fact_arlm(arlm_df):
    """Loads Last Month data to PostgreSQL using the COPY command with in-memory CSV."""
    
    try:
        connection = connect_to_pgsql()
        cursor = connection.cursor()
    
        # Delete and update Last Month data
        # Get the current date
        current_date = datetime.now()
        # Get the last day of the current month
        _, last_day = calendar.monthrange(current_date.year, current_date.month)
        
        if current_date.day <= last_day:
            # Delete AR Data WHERE PostingDate=last month
            cursor.execute("""DELETE FROM "ar_gbs"."fact_ar" WHERE 
            "postingdate" >= (date_trunc('month', current_date) - INTERVAL '1 month')
            AND "postingdate" < date_trunc('month', current_date)""")

        # Convert DataFrame to an in-memory CSV (string)
        output = io.StringIO()
        arlm_df.to_csv(output, index=False, header=True)
        output.seek(0)  # Go to the beginning of the string buffer

        # Prepare COPY command
        copy_query = """
        COPY "ar_gbs"."fact_ar" (
            CustomerCode, SellingPriceList, SeriesName, DocEntry, DocNum, LineNum, 
            PostingDate, DueDate, DocumentDate, InvoiceType, ItemCode, Quantity, 
            Status, Price, LineTotal, DocDiscount, DiscountSum, RowDiscountPercentage, 
            DocTotal, WhsCode, BPBranch, Branch, Department, AgentCode, 
            AgentCommissionAmount, Project, Currency, GProfit, LDtotal, GrossTotal, 
            StPrice, StValue, INMPrice, TotalBasePrice_LPP, TotalGrossProfit_LPP, 
            GLActC, CogsActC)
        FROM stdin WITH CSV HEADER DELIMITER ',';
        """
        cursor.copy_expert(sql=copy_query, file=output)         # Execute COPY command
        connection.commit()

    except Exception as e:
        logging.info(f"Error occurred while loading data: {e}", exc_info=True)
        connection.rollback()
    finally:
        cursor.close()
        connection.close()
    
##################################################### Create Main DAG #########################################################
# default args for main dag
default_args = {
    'owner': 'airflow',
    'retries': 1,  # Consider enabling retries
    'retry_delay': timedelta(minutes=1),  # Delay between retries
    'depends_on_past': False
}

# create main dag
@dag(
     dag_id="ar_staging_etl_gbs",
     default_args=default_args, 
     schedule="@daily", 
     start_date=days_ago(1), 
     catchup=False) # only run today's instance.

def etl_pipeline():
    # Step 1 - Extract ARLM
    @task
    def extract_arlm_task():
        return extract_fact_arlm()
  
    # Setp 2 - Load ARLM
    @task
    def load_arlm_task(extracted_arlm_df):
        load_fact_arlm(extracted_arlm_df)

    # Step 3 - Extract ARCM
    @task
    def extract_arcm_task():
        return extract_fact_arcm()

    # Step 4 - Load ARCM
    @task
    def load_arcm_task(extracted_arcm_df):
        load_fact_arcm(extracted_arcm_df)

    ######## Define task dependencies #########
    extracted_arlm_df = extract_arlm_task()
    load_fact_arlm_task = load_arlm_task(extracted_arlm_df)

    extracted_arcm_df = extract_arcm_task()
    load_fact_arcm_task = load_arcm_task(extracted_arcm_df)
    
    extracted_arlm_df >> load_fact_arlm_task
    extracted_arcm_df >> load_fact_arcm_task

etl_pipeline()
