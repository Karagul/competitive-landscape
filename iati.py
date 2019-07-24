import numpy as np
import pandas as pd
import requests
from tqdm import tqdm
from config import Config as cfg
from misc import constants as const
from misc.helper import chainer, rem_non_sectors, dai_sectors_mapping, \
    sector_disbursement, camelcase_conversion, project_end_status, sector_percentage_splitter
from misc.service_logger import serviceLogger as logger


class IATIdata:
    base_url = "http://datastore.iatistandard.org/api/1/access/"

    reporting_orgs = []
    filtered_orgs_list = list(cfg['IATI']['reporting_orgs'].split(','))

    if len(filtered_orgs_list) > 1:
        for org in filtered_orgs_list:
            if filtered_orgs_list.index(org) != len(filtered_orgs_list) - 1:
                reporting_orgs.append(org + "%7C")
            else:
                reporting_orgs.append(org)
    else:
        reporting_orgs.append(filtered_orgs_list[0])

    files = {}
    files_list = list(cfg['IATI']['files'].split(','))
    if len(files_list) > 0:
        for file in files_list:
            if file == 'activity':
                files['activity'] = base_url + "activity.csv?reporting-org=" + ''.join(reporting_orgs) \
                                    + "&start-date__gt=" + cfg['IATI']['after_date'] + "&end-date__lt=" \
                                    + cfg['IATI']['before_date'] + "&stream=True"
            elif file == 'activity_by_sector':
                files['activity_by_sector'] = base_url + "activity/by_sector.csv?reporting-org=" \
                                              + ''.join(reporting_orgs) + "&start-date__gt=" \
                                              + cfg['IATI']['after_date'] + "&end-date__lt=" \
                                              + cfg['IATI']['before_date'] + "&stream=True"
            elif file == 'activity_by_region':
                files['activity_by_region'] = base_url + "activity/by_country.csv?reporting-org=" \
                                              + ''.join(reporting_orgs) + "&start-date__gt=" \
                                              + cfg['IATI']['after_date'] + "&end-date__lt=" \
                                              + cfg['IATI']['before_date'] + "&stream=True"
            elif file == 'transaction':
                files['transaction'] = base_url + "transaction.csv?reporting-org=" + ''.join(reporting_orgs) \
                                       + "&start-date__gt=" + cfg['IATI']['after_date'] + "&end-date__lt=" \
                                       + cfg['IATI']['before_date'] + "&stream=True"
            elif file == 'transaction_by_sector':
                files['transaction_by_sector'] = base_url + "transaction/by_sector.csv?reporting-org=" \
                                                 + ''.join(reporting_orgs) + "&start-date__gt=" \
                                                 + cfg['IATI']['after_date'] + "&end-date__lt=" \
                                                 + cfg['IATI']['before_date'] + "&stream=True"

            elif file == 'transaction_by_region':
                files['transaction_by_region'] = base_url + "transaction/by_country.csv?reporting-org=" \
                                                 + ''.join(reporting_orgs) + "&start-date__gt=" \
                                                 + cfg['IATI']['after_date'] + "&end-date__lt=" \
                                                 + cfg['IATI']['before_date'] + "&stream=True"
            else:
                logger.error(const.DOWNLOAD_FILE_NOT_SPECIFIED, exc_info=True)
                exit(1)

    def download(self):
        """
        to download data from IATI APIs
        :return: downloaded file saved into designated dir
        """

        for file in self.files.keys():
            filename = cfg['PATH']['download_dir'] + file
            url = self.files[file]
            try:
                # download the files
                with requests.get(url, stream=True) as r:
                    size = len(r.content)
                    chunk_size = int(cfg['PATH']['chunk_size'])
                    r.raise_for_status()
                    with open(filename, 'wb') as f:
                        for chunk in tqdm(iterable=r.iter_content(chunk_size=chunk_size), total=size / chunk_size,
                                          unit='KB'):
                            if chunk:  # filtering out keep-alive new chunks
                                f.write(chunk)
                                # f.flush()
                log_msg = "Downloaded " + file + " successfully."
                logger.info(log_msg)
                return 1
            except Exception as e:
                logger.error(const.INTERNAL_SERV_ERROR, exc_info=True)
                exit(1)

    def process(self):
        """
        to pre-process the data before pushing it to SQL DB
        :return: cleansed and processed dataframe
        """
        filename = cfg['PATH']['download_dir'] + 'transaction.csv'
        txn = pd.read_csv(filename)

        # filepath for saving prepared files
        save_filepath = cfg['PATH']['save_dir']

        logger.info('dataset loaded into memory successfully.')

        """
        [table 1 - sector details]: iati-identifier, sector-code, sector, sector-percentage, sector-vocabulary, sector-vocabulary-code,
            transaction_sector-code, transaction_sector, transaction_sector-vocabulary, transaction_sector-vocabulary-code

        [table 2 - txn details]: iati-identifier, transaction-type, transaction-date, default-currency, transaction-value,
            transaction_ref, transaction_value_currency, transaction_value_value-date

        [table 3 - implementor details]: iati-identifier, transaction_privider-org, transaction_provider-org_ref,
            transaction_provider-org_provider-activity-id, transaction_receiver-org, transaction_receiver-org_ref,
            transaction_receiver-org_receiver-activity-id, participating-org (Implementing), participating-org-ref (Implementing),
            participating-org-type (Implementing), participating-org-type-code (Implementing)

        [table 4 - region,country details]: iati-identifier, recipient-country-code, recipient-country, recipient-country-percentage,
            recipient-region-code,recipient-region,recipient-region-percentage

        [table 5 - project details]: iati-identifier, <rest all fields>
        """

        tbl_sectors = txn.loc[:, ['iati-identifier', 'sector-code', 'sector', 'sector-percentage', 'sector-vocabulary',
                           'sector-vocabulary-code',
                           'transaction_sector-code', 'transaction_sector', 'transaction_sector-vocabulary',
                           'transaction_sector-vocabulary-code', 'transaction-date']]

        tbl_transactions = txn.loc[:, ['iati-identifier', 'transaction-type', 'transaction-date', 'default-currency',
                                       'transaction-value', 'transaction_ref', 'transaction_value_currency',
                                       'transaction_value_value-date']]

        tbl_implementors = txn.loc[:, ['iati-identifier', 'transaction_provider-org', 'transaction_provider-org_ref',
                                'transaction_provider-org_provider-activity-id', 'transaction_receiver-org',
                                'transaction_receiver-org_ref', 'transaction_receiver-org_receiver-activity-id',
                                       'participating-org (Implementing)', 'participating-org-ref (Implementing)',
                                       'participating-org-type (Implementing)',
                                       'participating-org-type-code (Implementing)']]

        tbl_regions = txn.loc[:, ['iati-identifier', 'recipient-country-code', 'recipient-country', 'recipient-country-percentage',
                              'recipient-region-code', 'recipient-region', 'recipient-region-percentage',
                              'transaction_recipient-country-code', 'transaction_recipient-country',
                              'transaction_recipient-region-code', 'transaction_recipient-region']]

        tbl_projects = txn.loc[:, ['iati-identifier', 'hierarchy', 'last-updated-datetime', 'default-language', 'reporting-org',
             'reporting-org-ref', 'reporting-org-type', 'reporting-org-type-code', 'title', 'description',
             'activity-status-code', 'start-planned', 'end-planned', 'start-actual', 'end-actual',
             'participating-org (Accountable)', 'participating-org-ref (Accountable)',
             'participating-org-type (Accountable)', 'participating-org-type-code (Accountable)',
             'participating-org (Funding)', 'participating-org-ref (Funding)',
             'participating-org-type (Funding)', 'participating-org-type-code (Funding)',
             'participating-org (Extending)', 'participating-org-ref (Extending)',
             'participating-org-type (Extending)', 'participating-org-type-code (Extending)',
             'collaboration-type-code', 'default-finance-type-code', 'default-flow-type-code',
             'default-aid-type-code', 'default-tied-status-code']]

        logger.info("Spliting into 1-NF Normalization forms.")

        logger.info("Processing transactions.")

        tbl_transactions.drop(['transaction_value_currency', 'transaction_value_value-date'], axis=1, inplace=True)

        txn_commit = tbl_transactions.loc[tbl_transactions['transaction-type'] == 2] \
            .drop(['transaction-type'], axis=1) \
            .rename(columns={'transaction-value': 'commitments'})

        txn_disburse = tbl_transactions.loc[tbl_transactions['transaction-type'] == 3] \
            .drop(['transaction-type'], axis=1) \
            .rename(columns={'transaction-value': 'disbursements'})

        txn_expend = tbl_transactions.loc[tbl_transactions['transaction-type'] == 4] \
            .drop(['transaction-type'], axis=1) \
            .rename(columns={'transaction-value': 'expenditures'})

        tbl_txn = txn_disburse.merge(txn_commit,
                                     on=['iati-identifier', 'transaction-date', 'transaction_ref', 'default-currency'],
                                     how='outer').merge(txn_expend,
                                                        on=['iati-identifier', 'transaction-date', 'transaction_ref',
                                                            'default-currency'],
                                                        how='outer')

        tbl_total_txn = tbl_txn.groupby(by=['iati-identifier', 'default-currency']).sum() \
            .reset_index() \
            .rename(columns={'commitments': 'total-Commitment', 'disbursements': 'total-Disbursement',
                             'expenditures': 'total-Expenditure'})

        # drop duplicate rows
        tbl_txn.drop_duplicates(keep='first', inplace=True)

        logger.info("Processing transactions....Done.")

        # write to csv - tbl_txn
        tbl_txn_filename = save_filepath + "IATI_transaction_details.csv"
        tbl_txn.to_csv(tbl_txn_filename, index=False)

        logger.info("Saved IATI_transaction_details.csv to disk.")

        # create dataframe for time-series slicer for activities as per txn-date
        id_txn_timeline = tbl_txn.loc[~tbl_txn['transaction-date'].isna(), ['iati-identifier', 'transaction-date']]
        id_txn_timeline['txn_year'] = id_txn_timeline['transaction-date'].apply(lambda x: x.split('-')[0])
        id_txn_timeline['txn_month'] = id_txn_timeline['transaction-date'].apply(lambda x: x.split('-')[1])
        id_txn_timeline['txn_day'] = id_txn_timeline['transaction-date'].apply(lambda x: x.split('-')[2])
        id_txn_timeline.drop_duplicates(keep='first', inplace=True)

        # write to csv - projects over transaction timeline
        proj_over_timeline_filename = save_filepath + "IATI_activities_over_timeline.csv"
        id_txn_timeline.to_csv(proj_over_timeline_filename, index=False)

        logger.info("Saved IATI_activities_over_timeline.csv to disk.")

        # write to csv - tbl_total_txn
        tbl_total_txn_filename = save_filepath + "IATI_transaction_values.csv"
        tbl_total_txn.to_csv(tbl_total_txn_filename, index=False)

        logger.info("Saved IATI_transaction_values.csv to disk.")

        logger.info("Processing implementors.")

        tbl_implementors['implementors'] = tbl_implementors['transaction_receiver-org'].fillna(
            value=tbl_implementors['participating-org (Implementing)'])

        tbl_implementors.implementors.fillna(value='Unknown', inplace=True)

        tbl_implementors.drop(['transaction_provider-org',
                               'transaction_provider-org_ref',
                               'transaction_provider-org_provider-activity-id',
                               'transaction_receiver-org_ref', 'participating-org-type-code (Implementing)',
                               'transaction_receiver-org_receiver-activity-id', 'participating-org-ref (Implementing)'],
                              axis=1, inplace=True)

        # drop duplicate rows
        tbl_implementors.drop_duplicates(keep='first', inplace=True)

        logger.info("Processing implementors....Done.")

        # write to csv - implementors
        imple_filename = save_filepath + "IATI_implementors.csv"
        tbl_implementors.to_csv(imple_filename, index=False)

        logger.info("Saved IATI_implementors.csv to disk.")

        logger.info("Processing regions and countries.")

        # clean some of the country names
        tbl_regions['recipient-country'].replace(to_replace="Congo, The Democratic Republic Of The",
                                                 value="Democratic Republic of the Congo", inplace=True)
        tbl_regions['recipient-country'].replace(to_replace="Cã\x94Te D'Ivoire",
                                                 value="Côte d'Ivoire", inplace=True)
        tbl_regions['recipient-country'].replace(to_replace="Iran, Islamic Republic Of",
                                                 value="Iran", inplace=True)
        tbl_regions['recipient-country'].replace(to_replace="Macedonia, The Former Yugoslav Republic Of",
                                                 value="Former Yugoslav Republic of Macedonia", inplace=True)
        tbl_regions['recipient-country'].replace(to_replace="Micronesia, Federated States Of",
                                                 value="Micronesia", inplace=True)
        tbl_regions['recipient-country'].replace(to_replace="Tanzania, United Republic Of",
                                                 value="Tanzania", inplace=True)
        tbl_regions['recipient-country'].replace(to_replace="Saint Helena, Ascension And Tristan Da Cunha",
                                                 value="Saint Helena", inplace=True)
        tbl_regions['recipient-country'].replace(to_replace="Venezuela, Bolivarian Republic Of",
                                                 value="Venezuela", inplace=True)
        tbl_regions['recipient-country'].replace(to_replace="Korea, Democratic People'S Republic Of",
                                                 value="Democratic People's Republic of Korea", inplace=True)
        tbl_regions['recipient-country'].replace(to_replace="Korea, Republic Of",
                                                 value="Democratic People's Republic of Korea", inplace=True)
        tbl_regions['recipient-country'].replace(to_replace="Curaã\x87Ao",
                                                 value="Curacao", inplace=True)

        tbl_regions.drop(['recipient-country-percentage', 'recipient-region-percentage',
                          'transaction_recipient-country-code', 'transaction_recipient-country',
                          'transaction_recipient-region-code', 'transaction_recipient-region'], axis=1, inplace=True)

        dac_regions_filepath = cfg['PATH']['dai_region_def_file']
        dac_regions = pd.read_excel(dac_regions_filepath, sheet_name='Sheet1')
        dac_regions.columns = ['recipient_code', 'recipient_name(en)', 'recipient_name(fr)', 'ISO_code', 'DAI_regions']

        # map country names to DAI region's definitions
        tbl_regions['recipient-country-lcase'] = tbl_regions['recipient-country'].str.strip().str.lower()
        dac_regions['recipient_name_en_lcase'] = dac_regions['recipient_name(en)'].str.strip().str.lower()

        tbl_regions = tbl_regions.merge(dac_regions, how='left', left_on='recipient-country-lcase',
                                        right_on='recipient_name_en_lcase')

        tbl_regions.drop(
            ['recipient-country-code', 'recipient-region-code', 'recipient-country-lcase', 'recipient_code',
             'recipient_name(fr)', 'recipient_name_en_lcase', 'recipient_name(en)'], axis=1, inplace=True)

        # drop duplicate rows
        tbl_regions.drop_duplicates(keep='first', inplace=True)

        logger.info("Processing regions and countries....Done.")

        # write to csv - regions
        region_filename = save_filepath + "IATI_region_details.csv"
        tbl_regions.to_csv(region_filename, index=False)

        logger.info("Saved IATI_region_details.csv to disk")

        logger.info("Processing project details.")

        # correct default-tied-status
        tbl_projects['default-tied-status-code'].fillna('5', inplace=True)
        tbl_projects['default-tied-status-code'] = tbl_projects['default-tied-status-code'].str.lower()
        tbl_projects['default-tied-status-code'] = tbl_projects['default-tied-status-code'].apply(
            lambda x: str(x).replace('nan', '5'))
        tbl_projects['default-tied-status-code'] = tbl_projects['default-tied-status-code'].apply(
            lambda x: str(x).replace('untied', '5'))
        tbl_projects['default-tied-status-code'] = tbl_projects['default-tied-status-code'].apply(
            lambda x: str(x).replace('tied', '4'))
        tbl_projects['default-tied-status-code'] = tbl_projects['default-tied-status-code'].apply(
            lambda x: str(x).replace('partially tied', '3'))
        tbl_projects['default-tied-status-code'] = tbl_projects['default-tied-status-code'].apply(
            lambda x: str(x).replace('u', '5'))
        tbl_projects['default-tied-status-code'] = tbl_projects['default-tied-status-code'].apply(
            lambda x: str(x).replace('t', '4'))
        tbl_projects['default-tied-status-code'] = tbl_projects['default-tied-status-code'].apply(
            lambda x: str(x).replace('p', '3'))
        tbl_projects['default-tied-status-code'] = tbl_projects['default-tied-status-code'].astype(int)

        # fill nan with the least possible date in pandas i.e. 1677-09-22
        tbl_projects['start-actual'] = tbl_projects['start-actual'].fillna(
            datetime.strftime(pd.Timestamp.min.ceil('D'), "%d-%m-%Y"))
        tbl_projects['start-planned'] = tbl_projects['start-planned'].fillna(
            datetime.strftime(pd.Timestamp.min.ceil('D'), "%d-%m-%Y"))
        tbl_projects['end-actual'] = tbl_projects['end-actual'].fillna(
            datetime.strftime(pd.Timestamp.min.ceil('D'), "%d-%m-%Y"))
        tbl_projects['end-planned'] = tbl_projects['end-planned'].fillna(
            datetime.strftime(pd.Timestamp.min.ceil('D'), "%d-%m-%Y"))

        # create 'start' & 'end' fields
        tbl_projects['start'] = np.where(
            tbl_projects['start-actual'].astype('datetime64[ns]') >= tbl_projects['start-planned'].astype(
                'datetime64[ns]'),
            tbl_projects['start-actual'], tbl_projects['start-planned']
        )
        tbl_projects['end'] = np.where(
            tbl_projects['end-actual'].astype('datetime64[ns]') >= tbl_projects['end-planned'].astype('datetime64[ns]'),
            tbl_projects['end-actual'], tbl_projects['end-planned']
        )

        # create timline categories, when did the project end?: 'earlier than 5 yrs', 'last 5 yrs' and 'still active'
        tbl_projects['project_end_status'] = tbl_projects['end'].apply(lambda x: project_end_status(x))

        # clean reporting-org names
        tbl_projects['reporting-org'] = tbl_projects['reporting-org'].apply(lambda x: x.replace('¿', '-'))
        tbl_projects['default-aid-type-code'].fillna('Unknown', inplace=True)

        tbl_projects.drop(
            ['default-language', 'reporting-org-ref', 'reporting-org-type', 'reporting-org-type-code', 'description',
             'participating-org-ref (Accountable)', 'participating-org-type (Accountable)',
             'participating-org-type-code (Accountable)', 'participating-org-type-code (Funding)',
             'participating-org (Extending)', 'participating-org-ref (Extending)', 'participating-org-type (Extending)',
             'participating-org-type-code (Extending)', 'collaboration-type-code', 'default-finance-type-code',
             'default-flow-type-code'], axis=1, inplace=True)

        # drop duplicate records
        tbl_projects.drop_duplicates(keep='first', inplace=True)

        logger.info("Processing project details....Done.")

        # write to csv - project details
        proj_filename = save_filepath + "IATI_project_details.csv"
        tbl_projects.to_csv(proj_filename, index=False)

        logger.info("Saved IATI_project_details.csv to disk.")

        logger.info("Processing sectors.")

        tbl_sectors.drop(['sector-vocabulary', 'sector-vocabulary-code', 'transaction_sector-vocabulary',
                          'transaction_sector-vocabulary-code'], axis=1, inplace=True)

        # where txn_sector-code is present but sector-code is null, impute txn_sector-code
        tbl_sectors['sector-code'].fillna(value=
                                          tbl_sectors.loc[tbl_sectors['sector-code'].isna(),
                                                          'transaction_sector-code'],
                                          inplace=True)

        # where txn_sector is present but sector is null, impute txn_sector
        tbl_sectors['sector'].fillna(value=
                                     tbl_sectors.loc[tbl_sectors['sector'].isna(),
                                                     'transaction_sector'],
                                     inplace=True)

        # where sector-percentage is null but sector-code is present, impute equal %age across sectors
        tbl_sectors.loc[(tbl_sectors['sector-percentage'].isna()) & (~tbl_sectors['sector-code'].isna()),
                        'sector-percentage'] = tbl_sectors.loc[(tbl_sectors['sector-percentage'].isna()) &
                                                               (~tbl_sectors['sector-code'].isna()),
                                                               'sector-code'].apply(
            lambda x: sector_percentage_splitter(len(x.split(';')))
        )

        # filling null for remaining sector-code & sector-percentage
        tbl_sectors.loc[(tbl_sectors['sector-code'].isna()) & (tbl_sectors['sector-percentage'].isna()),
                        ['sector-code', 'sector-percentage']] = '0', '100'

        # fillna in sector
        tbl_sectors['sector'].fillna(value='Unknown', inplace=True)

        # drop duplicate rows
        tbl_sectors.drop(['transaction_sector-code', 'transaction_sector'], axis=1, inplace=True)

        # exploding single cell into multiple rows
        lens = tbl_sectors['sector-code'].str.split(';').map(len)

        sectors = pd.DataFrame({'iati_identifier': np.repeat(tbl_sectors['iati-identifier'], lens),
                                'sector_code': chainer(tbl_sectors['sector-code']),
                                'sector_percentage': chainer(tbl_sectors['sector-percentage']),
                                'sector': np.repeat(tbl_sectors['sector'], lens),
                                'transaction_date': np.repeat(tbl_sectors['transaction-date'], lens)
                                })

        # replace wrongly reported sector codes
        try:
            sectors['sector_code'] = sectors['sector_code'].astype(str).str.replace('15120', '15111')
            sectors['sector_code'] = sectors['sector_code'].astype(str).str.replace('23010', '23110')
            sectors['sector_code'] = sectors['sector_code'].astype(str).str.replace('23067', '23230')
            sectors['sector_code'] = sectors['sector_code'].astype(str).str.replace('23064', '23510')
            sectors['sector_code'] = sectors['sector_code'].astype(str).str.replace('23040', '23630')
            sectors['sector_code'] = sectors['sector_code'].astype(str).str.replace('23030', '23210')
            sectors['sector_code'] = sectors['sector_code'].astype(str).str.replace('73000', '15110')

        except Exception as e:
            logger.error(const.DOESNT_EXISTS, exc_info=True)
            print("sector code doesn't exits")

        # remove all characters from sector-codes
        sectors.loc[sectors['sector_code'] == 'N/A', 'sector_code'] = 0
        sectors.loc[sectors['sector_code'] == 'nan', 'sector_code'] = 0
        sectors['sector_code'] = pd.to_numeric(sectors['sector_code']).astype(int)

        # remove all rows where sector-codes are not 5-digit or 7 digit codes.
        sectors['sector_code'] = sectors['sector_code'].astype(int)
        sectors['sector_code'] = sectors['sector_code'].apply(lambda x: rem_non_sectors(x))

        # remove all rows with sector-code info missing, i.e., sector-code != 0
        sectors = sectors.loc[sectors['sector_code'] != 0]

        # mapping DAI sector definitions to activity
        sectors['dai_sector'] = sectors['sector_code'].apply(lambda x: dai_sectors_mapping(x, 'Level0', digits=5))
        sectors['sector_category_code'] = sectors['sector_code'].apply(lambda x: dai_sectors_mapping(x, 'Level1_code',
                                                                                                     digits=5))
        sectors['sector_category'] = sectors['sector_code'].apply(lambda x: dai_sectors_mapping(x, 'Level1', digits=5))
        sectors['iati_sector_code'] = sectors['sector_code'].apply(lambda x: dai_sectors_mapping(x, 'Level2_code',
                                                                                                 digits=5))
        sectors['iati_sector'] = sectors['sector_code'].apply(lambda x: dai_sectors_mapping(x, 'Level2', digits=5))
        sectors['subsector_code'] = sectors['sector_code'].apply(lambda x: dai_sectors_mapping(x, 'Level3_code',
                                                                                               digits=7))
        sectors['subsector'] = sectors['sector_code'].apply(lambda x: dai_sectors_mapping(x, 'Level3', digits=7))

        # replace the NaN created due to non 5 or 7 digit sector codes with 'unknown' & 0
        sectors['dai_sector'].fillna(value='Unknown', inplace=True)
        sectors['sector_category'].fillna(value='Unknown', inplace=True)
        sectors['iati_sector'].fillna(value='Unknown', inplace=True)
        sectors['subsector'].fillna(value='Unknown', inplace=True)
        sectors['sector_category_code'].fillna(value=0, inplace=True)
        sectors['iati_sector_code'].fillna(value=0, inplace=True)
        sectors['subsector_code'].fillna(value=0, inplace=True)

        # drop duplicate rows
        sectors.drop_duplicates(keep='first', inplace=True)

        logger.info("Processing sectors....Done.")

        # write to csv - sectors
        sector_filename = save_filepath + "IATI_sector_details.csv"
        sectors.to_csv(sector_filename, index=False)

        logger.info("Saved IATI_sector_details.csv to disk.")

        return tbl_txn, id_txn_timeline, tbl_total_txn, tbl_implementors, tbl_regions, tbl_projects, sectors