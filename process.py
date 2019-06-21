from datetime import datetime
import sqlalchemy
import pandas as pd
from iati import IATIdata
from misc.service_logger import serviceLogger as logger
from misc.db_connection import engine

if __name__ == "__main__":
    iati = IATIdata()
    # iati.download()
    txn_data, ids_and_yrs = iati.process()

    # add date of loading the data files to DB
    txn_data['db_load_date'] = datetime.now().strftime("%d-%m-%Y %H:%M:%S")
    ids_and_yrs['db_load_date'] = datetime.now().strftime("%d-%m-%Y %H:%M:%S")

    # The problem is that SQL Server stored procedures (including system stored procedures like sp_prepexec)
    # are limited to 2100 parameters, so if the DataFrame has 100 columns then to_sql can only insert
    # about 20 rows at a time.
    chunk_size = 2097 // len(ids_and_yrs)
    chunk_size = 1000 if chunk_size > 1000 else chunk_size

    # write dataframes to SQL tables
    ids_and_yrs.to_sql(name='iati_activity_over_timeline', con=engine, schema='dbo', if_exists='append', index=False,
                       index_label=list(ids_and_yrs.columns),
                       dtype={'iati-identifier': sqlalchemy.types.NVARCHAR(length=200),
                              'txn-years': sqlalchemy.types.CHAR(length=4),
                              'txn-month': sqlalchemy.types.CHAR(length=2)},
                       chunksize=chunk_size, method='multi')

    logger.info("Pushed activities carried out over the timeline data (activity_over_timeline) to Azure SQL database")

    chunk_size = 2097 // len(txn_data)
    chunk_size = 1000 if chunk_size > 1000 else chunk_size
    txn_data.to_sql(name='iati_txn', con=engine, schema='dbo', if_exists='append', index=False,
                    index_label=['iati_identifier',
                                 'hierarchy',
                                 'last_updated_datetime',
                                 'sector_code',
                                 'sector',
                                 'sector_percentage',
                                 'dac_sector',
                                 'sector_category',
                                 'txn_date',
                                 'txn_type',
                                 'txn_currency',
                                 'default_currency',
                                 'txn_value',
                                 'txn_receiver_org',
                                 'reporting_org',
                                 'participating_org_funding',
                                 'participating_org_funding_type',
                                 'participating_org_implementing',
                                 'participating_org_implementing_type',
                                 'implementor',
                                 'multilateral',
                                 'title',
                                 'description',
                                 'start_actual',
                                 'start_planned',
                                 'end_actual',
                                 'end_planned',
                                 'start_date',
                                 'end_date',
                                 'sector_txn_value',
                                 'default_aid_type_code',
                                 'recipient_country',
                                 'recipient_country_code',
                                 'recipient_region',
                                 'recipient_region_code',
                                 'dac_country_name',
                                 'dac_region_name'],
                    dtype={'iati_identifier': sqlalchemy.types.NVARCHAR(length=200),
                           'hierarchy': sqlalchemy.types.INTEGER,
                           'last_updated_datetime': sqlalchemy.types.CHAR(length=10),
                           'sector_code': sqlalchemy.types.INTEGER,
                           'sector': sqlalchemy.types.NVARCHAR(length=250),
                           'sector_percentage': sqlalchemy.types.DECIMAL(precision=4),
                           'dac_sector': sqlalchemy.types.NVARCHAR(length=200),
                           'sector_category': sqlalchemy.types.NVARCHAR(length=200),
                           'txn_date': sqlalchemy.types.CHAR(length=10),
                           'txn_type': sqlalchemy.types.INTEGER,
                           'txn_currency': sqlalchemy.types.CHAR(3),
                           'default_currency': sqlalchemy.types.CHAR(3),
                           'txn_value': sqlalchemy.types.DECIMAL(precision=4),
                           'txn_receiver_org': sqlalchemy.types.NVARCHAR(length=200),
                           'reporting_org': sqlalchemy.types.NVARCHAR(length=200),
                           'participating_org_funding': sqlalchemy.types.NVARCHAR(length=200),
                           'participating_org_funding_type': sqlalchemy.types.NVARCHAR(length=200),
                           'participating_org_implementing': sqlalchemy.types.NVARCHAR(length=200),
                           'participating_org_implementing_type': sqlalchemy.types.NVARCHAR(length=200),
                           'implementor': sqlalchemy.types.NVARCHAR(length=200),
                           'multilateral': sqlalchemy.types.NVARCHAR(length=200),
                           'title': sqlalchemy.types.NVARCHAR,
                           'description': sqlalchemy.types.NVARCHAR,
                           'start_actual': sqlalchemy.types.CHAR(length=10),
                           'start_planned': sqlalchemy.types.CHAR(length=10),
                           'end_actual': sqlalchemy.types.CHAR(length=10),
                           'end_planned': sqlalchemy.types.CHAR(length=10),
                           'start_date': sqlalchemy.types.CHAR(length=10),
                           'end_date': sqlalchemy.types.CHAR(length=10),
                           'sector_txn_value': sqlalchemy.types.DECIMAL(precision=4),
                           'default_aid_type_code': sqlalchemy.types.CHAR(length=3),
                           'recipient_country': sqlalchemy.types.NVARCHAR(length=150),
                           'recipient_country_code': sqlalchemy.types.CHAR(length=20),
                           'recipient_region': sqlalchemy.types.NVARCHAR(length=150),
                           'recipient_region_code': sqlalchemy.types.CHAR(length=20),
                           'dac_country_name': sqlalchemy.types.NVARCHAR(length=150),
                           'dac_region_name': sqlalchemy.types.NVARCHAR(length=150)},
                    chunksize=chunk_size, method='multi'
                    )

    logger.info("Pushed transaction data (txn) to Azure SQL database")

    txn_raw = pd.read_csv("dataset/raw_data/transaction.csv")
    chunk_size = 2097 // len(txn_raw)
    chunk_size = 1000 if chunk_size > 1000 else chunk_size
    txn_raw.to_sql(name='iati_txn', con=engine, schema='dbo', if_exists='append', index=False,
                   index_label=["iati_identifier",
                                "transaction_type",
                                "transaction_date",
                                "default_currency",
                                "transaction_value",
                                "transaction_ref",
                                "transaction_value_currency",
                                "transaction_value_value_date",
                                "transaction_provider_org",
                                "transaction_provider_org_ref",
                                "transaction_provider_org_provider_activity_id",
                                "transaction_receiver_org",
                                "transaction_receiver_org_ref",
                                "transaction_receiver_org_receiver_activity_id",
                                "transaction_description",
                                "transaction_flow_type_code",
                                "transaction_finance_type_code",
                                "transaction_aid_type_code",
                                "transaction_tied_status_code",
                                "transaction_disbursement_channel_code",
                                "transaction_recipient_country_code",
                                "transaction_recipient_country",
                                "transaction_recipient_region_code",
                                "transaction_recipient_region",
                                "transaction_sector_code",
                                "transaction_sector",
                                "transaction_sector_vocabulary",
                                "transaction_sector_vocabulary_code",
                                "iati_identifier",
                                "hierarchy",
                                "last_updated_datetime",
                                "default_language",
                                "reporting_org",
                                "reporting_org_ref",
                                "reporting_org_type",
                                "reporting_org_type_code",
                                "title",
                                "description",
                                "activity_status_code",
                                "start_planned",
                                "end_planned",
                                "start_actual",
                                "end_actual",
                                "participating_org (Accountable)",
                                "participating_org_ref (Accountable)",
                                "participating_org_type (Accountable)",
                                "participating_org_type_code (Accountable)",
                                "participating_org (Funding)",
                                "participating_org_ref (Funding)",
                                "participating_org_type (Funding)",
                                "participating_org_type_code (Funding)",
                                "participating_org (Extending)",
                                "participating_org_ref (Extending)",
                                "participating_org_type (Extending)",
                                "participating_org_type_code (Extending)",
                                "participating_org (Implementing)",
                                "participating_org_ref (Implementing)",
                                "participating_org_type (Implementing)",
                                "participating_org_type_code (Implementing)",
                                "recipient_country_code",
                                "recipient_country",
                                "recipient_country_percentage",
                                "recipient_region_code",
                                "recipient_region",
                                "recipient_region_percentage",
                                "sector_code",
                                "sector",
                                "sector_percentage",
                                "sector_vocabulary",
                                "sector_vocabulary_code",
                                "collaboration_type_code",
                                "default_finance_type_code",
                                "default_flow_type_code",
                                "default_aid_type_code",
                                "default_tied_status_code"],
                   dtype={"iati_identifier": sqlalchemy.types.NVARCHAR(length=200),
                          "transaction_type": sqlalchemy.types.INTEGER,
                          "transaction_date": sqlalchemy.types.CHAR(length=10),
                          "default_currency": sqlalchemy.types.CHAR(length=3),
                          "transaction_value": sqlalchemy.types.DECIMAL(precision=4),
                          "transaction_ref": sqlalchemy.types.NVARCHAR(length=200),
                          "transaction_value_currency": sqlalchemy.types.CHAR(length=3),
                          "transaction_value_value_date": sqlalchemy.types.CHAR(length=10),
                          "transaction_provider_org": sqlalchemy.types.NVARCHAR(length=200),
                          "transaction_provider_org_ref": sqlalchemy.types.NVARCHAR(length=200),
                          "transaction_provider_org_provider_activity_id": sqlalchemy.types.NVARCHAR(length=200),
                          "transaction_receiver_org": sqlalchemy.types.NVARCHAR(length=200),
                          "transaction_receiver_org_ref": sqlalchemy.types.NVARCHAR(length=200),
                          "transaction_receiver_org_receiver_activity_id": sqlalchemy.types.NVARCHAR(length=200),
                          "transaction_description": sqlalchemy.types.NVARCHAR,
                          "transaction_flow_type_code": sqlalchemy.types.CHAR(length=5),
                          "transaction_finance_type_code": sqlalchemy.types.CHAR(length=5),
                          "transaction_aid_type_code": sqlalchemy.types.CHAR(length=5),
                          "transaction_tied_status_code": sqlalchemy.types.CHAR(length=5),
                          "transaction_disbursement_channel_code": sqlalchemy.types.CHAR(length=5),
                          "transaction_recipient_country_code": sqlalchemy.types.NVARCHAR(length=50),
                          "transaction_recipient_country": sqlalchemy.types.NVARCHAR(length=150),
                          "transaction_recipient_region_code": sqlalchemy.types.NVARCHAR(length=50),
                          "transaction_recipient_region": sqlalchemy.types.NVARCHAR(length=150),
                          "transaction_sector_code": sqlalchemy.types.INTEGER,
                          "transaction_sector": sqlalchemy.types.NVARCHAR(length=200),
                          "transaction_sector_vocabulary": sqlalchemy.types.NVARCHAR(length=200),
                          "transaction_sector_vocabulary_code": sqlalchemy.types.NVARCHAR(length=200),
                          "hierarchy": sqlalchemy.types.CHAR(length=2),
                          "last_updated_datetime": sqlalchemy.types.CHAR(length=10),
                          "default_language": sqlalchemy.types.CHAR(length=4),
                          "reporting_org": sqlalchemy.types.NVARCHAR(length=200),
                          "reporting_org_ref": sqlalchemy.types.NVARCHAR(length=200),
                          "reporting_org_type": sqlalchemy.types.NVARCHAR(length=200),
                          "reporting_org_type_code": sqlalchemy.types.NVARCHAR(length=150),
                          "title": sqlalchemy.types.NVARCHAR,
                          "description": sqlalchemy.types.NVARCHAR,
                          "activity_status_code": sqlalchemy.types.NVARCHAR(length=100),
                          "start_planned": sqlalchemy.types.CHAR(10),
                          "end_planned": sqlalchemy.types.CHAR(10),
                          "start_actual": sqlalchemy.types.CHAR(10),
                          "end_actual": sqlalchemy.types.CHAR(10),
                          "participating_org (Accountable)": sqlalchemy.types.NVARCHAR(200),
                          "participating_org_ref (Accountable)": sqlalchemy.types.NVARCHAR(200),
                          "participating_org_type (Accountable)": sqlalchemy.types.NVARCHAR(200),
                          "participating_org_type_code (Accountable)": sqlalchemy.types.NVARCHAR(200),
                          "participating_org (Funding)": sqlalchemy.types.NVARCHAR(200),
                          "participating_org_ref (Funding)": sqlalchemy.types.NVARCHAR(200),
                          "participating_org_type (Funding)": sqlalchemy.types.NVARCHAR(200),
                          "participating_org_type_code (Funding)": sqlalchemy.types.NVARCHAR(200),
                          "participating_org (Extending)": sqlalchemy.types.NVARCHAR(200),
                          "participating_org_ref (Extending)": sqlalchemy.types.NVARCHAR(200),
                          "participating_org_type (Extending)": sqlalchemy.types.NVARCHAR(200),
                          "participating_org_type_code (Extending)": sqlalchemy.types.NVARCHAR(200),
                          "participating_org (Implementing)": sqlalchemy.types.NVARCHAR(200),
                          "participating_org_ref (Implementing)": sqlalchemy.types.NVARCHAR(200),
                          "participating_org_type (Implementing)": sqlalchemy.types.NVARCHAR(200),
                          "participating_org_type_code (Implementing)": sqlalchemy.types.NVARCHAR(200),
                          "recipient_country_code": sqlalchemy.types.NVARCHAR(150),
                          "recipient_country": sqlalchemy.types.NVARCHAR(150),
                          "recipient_country_percentage": sqlalchemy.types.NVARCHAR(150),
                          "recipient_region_code": sqlalchemy.types.NVARCHAR(150),
                          "recipient_region": sqlalchemy.types.NVARCHAR(150),
                          "recipient_region_percentage": sqlalchemy.types.NVARCHAR(150),
                          "sector_code": sqlalchemy.types.NVARCHAR(150),
                          "sector": sqlalchemy.types.NVARCHAR,
                          "sector_percentage": sqlalchemy.types.NVARCHAR(150),
                          "sector_vocabulary": sqlalchemy.types.NVARCHAR(150),
                          "sector_vocabulary_code": sqlalchemy.types.NVARCHAR(150),
                          "collaboration_type_code": sqlalchemy.types.CHAR(50),
                          "default_finance_type_code": sqlalchemy.types.CHAR(50),
                          "default_flow_type_code": sqlalchemy.types.CHAR(50),
                          "default_aid_type_code": sqlalchemy.types.CHAR(50),
                          "default_tied_status_code": sqlalchemy.types.CHAR(50)},
                   chunksize=chunk_size, method='multi'
                   )
    engine.close() # close the db connection
    logger.info("Pushed raw transaction data (txn_raw) to Azure SQL database")