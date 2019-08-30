from datetime import datetime
import sqlalchemy
import pandas as pd
from iati import IATIdata
from misc.service_logger import serviceLogger as logger
from datetime import datetime
# from core.db_connection import engine
from config import Config as cfg

if __name__ == "__main__":
    iati = IATIdata()
    # iati.download()
    # transaction_details, activities_over_timeline, transaction_values, implementors, regions, projects, sectors = iati.process()
    implementors = iati.process()
    #
    #
    # # add date of loading the data files to DB
    # transaction_details['db_load_date'] = datetime.now().strftime("%d-%m-%Y %H:%M:%S")
    # activities_over_timeline['db_load_date'] = datetime.now().strftime("%d-%m-%Y %H:%M:%S")
    # transaction_values['db_load_date'] = datetime.now().strftime("%d-%m-%Y %H:%M:%S")
    # implementors['db_load_date'] = datetime.now().strftime("%d-%m-%Y %H:%M:%S")
    # regions['db_load_date'] = datetime.now().strftime("%d-%m-%Y %H:%M:%S")
    # projects['db_load_date'] = datetime.now().strftime("%d-%m-%Y %H:%M:%S")
    # sectors['db_load_date'] = datetime.now().strftime("%d-%m-%Y %H:%M:%S")


    """
    The problem is that SQL Server stored procedures (including system stored procedures like sp_prepexec)
    are limited to 2100 parameters, so if the DataFrame has 100 columns then to_sql can only insert
    about 20 rows at a time.
    """
    # ids_and_yrs = pd.read_csv(filename) # unlock this and everything below for saving into DB
    # # write to DataBase [iati_activity_over_timeline]
    # chunk_size = len(ids_and_yrs) // 2097
    # chunk_size = 1000 if chunk_size > 1000 else chunk_size
    # ids_and_yrs.to_sql(name='iati_activity_over_timeline', con=engine, schema='dbo', if_exists='replace', index=False,
    #                    index_label=list(ids_and_yrs.columns),
    #                    dtype={'iati-identifier': sqlalchemy.types.NVARCHAR(length=200),
    #                           'txn-years': sqlalchemy.types.CHAR(length=4),
    #                           'txn-month': sqlalchemy.types.CHAR(length=2)},
    #                    chunksize=chunk_size, method='multi')
    #
    # logger.info("Pushed activities carried out over the timeline data (activity_over_timeline) to Azure SQL database")


    # txn_data = pd.read_csv(filename) # unlock this and everything below for saving into DB
    # # write to DataBase [iati_txn]
    # # chunk_size = len(txn_data) // 2097
    # # chunk_size = 1000 if chunk_size > 1000 else chunk_size
    # txn_data.to_sql(name='iati_txn', con=engine, schema='dbo', if_exists='replace', index=False,
    #                 dtype={'iati_identifier': sqlalchemy.types.NVARCHAR(length=400),
    #                        'hierarchy': sqlalchemy.types.INTEGER,
    #                        'last_updated_datetime': sqlalchemy.types.CHAR(length=10),
    #                        'sector_code': sqlalchemy.types.INTEGER,
    #                        'sector': sqlalchemy.types.NVARCHAR,
    #                        'sector_percentage': sqlalchemy.types.DECIMAL(precision=4),
    #                        'dac_sector': sqlalchemy.types.NVARCHAR(length=200),
    #                        'sector_category': sqlalchemy.types.NVARCHAR(length=200),
    #                        'txn_date': sqlalchemy.types.CHAR(length=10),
    #                        # 'txn_type': sqlalchemy.types.INTEGER,
    #                        'txn_currency': sqlalchemy.types.CHAR(3),
    #                        'default_currency': sqlalchemy.types.CHAR(3),
    #                        # 'txn_value': sqlalchemy.types.DECIMAL(precision=4),
    #                        'total-Commitment': sqlalchemy.types.DECIMAL(precision=4),
    #                        'total-Disbursement': sqlalchemy.types.DECIMAL(precision=4),
    #                        'total-Expenditure': sqlalchemy.types.DECIMAL(precision=4),
    #                        'txn_receiver_org': sqlalchemy.types.NVARCHAR,
    #                        'reporting_org': sqlalchemy.types.NVARCHAR(length=350),
    #                        'participating_org_funding': sqlalchemy.types.NVARCHAR,
    #                        'participating_org_funding_type': sqlalchemy.types.NVARCHAR,
    #                        'participating_org_implementing': sqlalchemy.types.NVARCHAR,
    #                        'participating_org_implementing_type': sqlalchemy.types.NVARCHAR,
    #                        'implementor': sqlalchemy.types.NVARCHAR(length=400),
    #                        'multilateral': sqlalchemy.types.NVARCHAR(length=200),
    #                        'title': sqlalchemy.types.NVARCHAR,
    #                        'description': sqlalchemy.types.NVARCHAR,
    #                        'start_actual': sqlalchemy.types.CHAR(length=10),
    #                        'start_planned': sqlalchemy.types.CHAR(length=10),
    #                        'end_actual': sqlalchemy.types.CHAR(length=10),
    #                        'end_planned': sqlalchemy.types.CHAR(length=10),
    #                        'start_date': sqlalchemy.types.CHAR(length=10),
    #                        'end_date': sqlalchemy.types.CHAR(length=10),
    #                        # 'sector_txn_value': sqlalchemy.types.DECIMAL(precision=4),
    #                        'sector-Commitment': sqlalchemy.types.DECIMAL(precision=4),
    #                        'sector-Disbursement': sqlalchemy.types.DECIMAL(precision=4),
    #                        'sector-Expenditure': sqlalchemy.types.DECIMAL(precision=4),
    #                        'default_aid_type_code': sqlalchemy.types.CHAR(length=3),
    #                        'recipient_country': sqlalchemy.types.NVARCHAR(length=350),
    #                        'recipient_country_code': sqlalchemy.types.CHAR(length=50),
    #                        'recipient_region': sqlalchemy.types.NVARCHAR(length=350),
    #                        'recipient_region_code': sqlalchemy.types.CHAR(length=50),
    #                        'dac_country_name': sqlalchemy.types.NVARCHAR(length=350),
    #                        'dac_region_name': sqlalchemy.types.NVARCHAR(length=350)},
    #                 chunksize=36, method='multi'
    #                 )
    #
    # logger.info("Pushed transaction data (txn) to Azure SQL database")

    # write to DataBase [iati_txn_raw]
    # txn_raw = pd.read_csv("dataset/raw_data/transaction.csv")
    # chunk_size = len(txn_raw) // 2097
    # chunk_size = 1000 if chunk_size > 1000 else chunk_size

    # txn_raw.to_sql(name='iati_txn', con=engine, schema='dbo', if_exists='replace', index=False,
    #                dtype={"iati_identifier": sqlalchemy.types.NVARCHAR(length=200),
    #                       "transaction_type": sqlalchemy.types.INTEGER,
    #                       "transaction_date": sqlalchemy.types.CHAR(length=10),
    #                       "default_currency": sqlalchemy.types.CHAR(length=3),
    #                       "transaction_value": sqlalchemy.types.DECIMAL(precision=4),
    #                       "transaction_ref": sqlalchemy.types.NVARCHAR,
    #                       "transaction_value_currency": sqlalchemy.types.CHAR(length=4),
    #                       "transaction_value_value_date": sqlalchemy.types.CHAR(length=10),
    #                       "transaction_provider_org": sqlalchemy.types.NVARCHAR,
    #                       "transaction_provider_org_ref": sqlalchemy.types.NVARCHAR,
    #                       "transaction_provider_org_provider_activity_id": sqlalchemy.types.NVARCHAR,
    #                       "transaction_receiver_org": sqlalchemy.types.NVARCHAR,
    #                       "transaction_receiver_org_ref": sqlalchemy.types.NVARCHAR,
    #                       "transaction_receiver_org_receiver_activity_id": sqlalchemy.types.NVARCHAR,
    #                       "transaction_description": sqlalchemy.types.NVARCHAR,
    #                       "transaction_flow_type_code": sqlalchemy.types.CHAR(length=5),
    #                       "transaction_finance_type_code": sqlalchemy.types.CHAR(length=5),
    #                       "transaction_aid_type_code": sqlalchemy.types.CHAR(length=5),
    #                       "transaction_tied_status_code": sqlalchemy.types.CHAR(length=5),
    #                       "transaction_disbursement_channel_code": sqlalchemy.types.CHAR(length=5),
    #                       "transaction_recipient_country_code": sqlalchemy.types.NVARCHAR(length=50),
    #                       "transaction_recipient_country": sqlalchemy.types.NVARCHAR(length=350),
    #                       "transaction_recipient_region_code": sqlalchemy.types.NVARCHAR(length=50),
    #                       "transaction_recipient_region": sqlalchemy.types.NVARCHAR(length=350),
    #                       "transaction_sector_code": sqlalchemy.types.INTEGER,
    #                       "transaction_sector": sqlalchemy.types.NVARCHAR,
    #                       "transaction_sector_vocabulary": sqlalchemy.types.NVARCHAR,
    #                       "transaction_sector_vocabulary_code": sqlalchemy.types.NVARCHAR,
    #                       "hierarchy": sqlalchemy.types.CHAR(length=2),
    #                       "last_updated_datetime": sqlalchemy.types.CHAR(length=10),
    #                       "default_language": sqlalchemy.types.CHAR(length=4),
    #                       "reporting_org": sqlalchemy.types.NVARCHAR,
    #                       "reporting_org_ref": sqlalchemy.types.NVARCHAR,
    #                       "reporting_org_type": sqlalchemy.types.NVARCHAR,
    #                       "reporting_org_type_code": sqlalchemy.types.NVARCHAR,
    #                       "title": sqlalchemy.types.NVARCHAR,
    #                       "description": sqlalchemy.types.NVARCHAR,
    #                       "activity_status_code": sqlalchemy.types.NVARCHAR,
    #                       "start_planned": sqlalchemy.types.CHAR(10),
    #                       "end_planned": sqlalchemy.types.CHAR(10),
    #                       "start_actual": sqlalchemy.types.CHAR(10),
    #                       "end_actual": sqlalchemy.types.CHAR(10),
    #                       "participating_org (Accountable)": sqlalchemy.types.NVARCHAR,
    #                       "participating_org_ref (Accountable)": sqlalchemy.types.NVARCHAR,
    #                       "participating_org_type (Accountable)": sqlalchemy.types.NVARCHAR,
    #                       "participating_org_type_code (Accountable)": sqlalchemy.types.NVARCHAR,
    #                       "participating_org (Funding)": sqlalchemy.types.NVARCHAR,
    #                       "participating_org_ref (Funding)": sqlalchemy.types.NVARCHAR,
    #                       "participating_org_type (Funding)": sqlalchemy.types.NVARCHAR,
    #                       "participating_org_type_code (Funding)": sqlalchemy.types.NVARCHAR,
    #                       "participating_org (Extending)": sqlalchemy.types.NVARCHAR,
    #                       "participating_org_ref (Extending)": sqlalchemy.types.NVARCHAR,
    #                       "participating_org_type (Extending)": sqlalchemy.types.NVARCHAR,
    #                       "participating_org_type_code (Extending)": sqlalchemy.types.NVARCHAR,
    #                       "participating_org (Implementing)": sqlalchemy.types.NVARCHAR,
    #                       "participating_org_ref (Implementing)": sqlalchemy.types.NVARCHAR,
    #                       "participating_org_type (Implementing)": sqlalchemy.types.NVARCHAR,
    #                       "participating_org_type_code (Implementing)": sqlalchemy.types.NVARCHAR,
    #                       "recipient_country_code": sqlalchemy.types.NVARCHAR,
    #                       "recipient_country": sqlalchemy.types.NVARCHAR,
    #                       "recipient_country_percentage": sqlalchemy.types.NVARCHAR,
    #                       "recipient_region_code": sqlalchemy.types.NVARCHAR,
    #                       "recipient_region": sqlalchemy.types.NVARCHAR,
    #                       "recipient_region_percentage": sqlalchemy.types.NVARCHAR,
    #                       "sector_code": sqlalchemy.types.NVARCHAR,
    #                       "sector": sqlalchemy.types.NVARCHAR,
    #                       "sector_percentage": sqlalchemy.types.NVARCHAR,
    #                       "sector_vocabulary": sqlalchemy.types.NVARCHAR,
    #                       "sector_vocabulary_code": sqlalchemy.types.NVARCHAR,
    #                       "collaboration_type_code": sqlalchemy.types.CHAR(length=50),
    #                       "default_finance_type_code": sqlalchemy.types.CHAR(length=50),
    #                       "default_flow_type_code": sqlalchemy.types.CHAR(length=50),
    #                       "default_aid_type_code": sqlalchemy.types.CHAR(length=50),
    #                       "default_tied_status_code": sqlalchemy.types.CHAR(length=50)},
    #                chunksize=36, method='multi'
    #                )
    #
    # logger.info("Pushed raw transaction data (txn_raw) to Azure SQL database")

    logger.info("Pipeline completely successfully.")
