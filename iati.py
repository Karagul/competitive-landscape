import numpy as np
import pandas as pd
import requests
from tqdm import tqdm
from config import Config as cfg
from misc import constants as const
from misc.helper import chainer, rem_non_sectors, dai_sectors_mapping, sector_disbursement, camelcase_conversion
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

        logger.info('data files loaded successfully into memory')

        # correct default-tied-status
        txn['default-tied-status-code'].fillna('5', inplace=True)
        txn['default-tied-status-code'] = txn['default-tied-status-code'].str.lower()
        txn['default-tied-status-code'] = txn['default-tied-status-code'].apply(lambda x: str(x).replace('nan', '5'))
        txn['default-tied-status-code'] = txn['default-tied-status-code'].apply(lambda x: str(x).replace('untied', '5'))
        txn['default-tied-status-code'] = txn['default-tied-status-code'].apply(lambda x: str(x).replace('tied', '4'))
        txn['default-tied-status-code'] = txn['default-tied-status-code'].apply(
            lambda x: str(x).replace('partially tied', '3'))
        txn['default-tied-status-code'] = txn['default-tied-status-code'].apply(lambda x: str(x).replace('u', '5'))
        txn['default-tied-status-code'] = txn['default-tied-status-code'].apply(lambda x: str(x).replace('t', '4'))
        txn['default-tied-status-code'] = txn['default-tied-status-code'].apply(lambda x: str(x).replace('p', '3'))
        txn['default-tied-status-code'] = txn['default-tied-status-code'].astype(int)

        logger.info("corrected wrongly placed tied-status-codes")

        # explodding one row into multiple rows

        # fillna in sector-code and sector-percentage
        txn['sector-code'].fillna(value='0', inplace=True)
        txn['sector'].fillna(value='unknown', inplace=True)
        txn['sector-percentage'].fillna(value='100', inplace=True)

        # calculate lengths of splits
        len_of_split = txn['sector-code'].str.split(';').map(len)

        # create new df, repeating everything else and chaining the field to split to length of len_of_split
        txn_df = pd.DataFrame({
            'iati-identifier': np.repeat(txn['iati-identifier'], len_of_split),
            'transaction-type': np.repeat(txn['transaction-type'], len_of_split),
            'transaction-date': np.repeat(txn['transaction-date'], len_of_split),
            'default-currency': np.repeat(txn['default-currency'], len_of_split),
            'transaction-value': np.repeat(txn['transaction-value'], len_of_split),
            'transaction_value_currency': np.repeat(txn['transaction_value_currency'], len_of_split),
            'transaction_receiver-org': np.repeat(txn['transaction_receiver-org'], len_of_split),
            'hierarchy': np.repeat(txn['hierarchy'], len_of_split),
            'last-updated-datetime': np.repeat(txn['last-updated-datetime'], len_of_split),
            'reporting-org': np.repeat(txn['reporting-org'], len_of_split),
            'title': np.repeat(txn['title'], len_of_split),
            'description': np.repeat(txn['description'], len_of_split),
            'activity-status-code': np.repeat(txn['activity-status-code'], len_of_split),
            'start-planned': np.repeat(txn['start-planned'], len_of_split),
            'start-actual': np.repeat(txn['start-actual'], len_of_split),
            'end-planned': np.repeat(txn['end-planned'], len_of_split),
            'end-actual': np.repeat(txn['end-actual'], len_of_split),
            'participating-org (Implementing)': np.repeat(txn['participating-org (Implementing)'], len_of_split),
            'participating-org-type (Implementing)': np.repeat(txn['participating-org-type (Implementing)'],
                                                               len_of_split),
            'participating-org (Funding)': np.repeat(txn['participating-org (Funding)'], len_of_split),
            'recipient-country': np.repeat(txn['recipient-country'], len_of_split),
            'recipient-country-code': np.repeat(txn['recipient-country-code'], len_of_split),
            'recipient-region': np.repeat(txn['recipient-region'], len_of_split),
            'recipient-region-code': np.repeat(txn['recipient-region-code'], len_of_split),
            'sector-code': chainer(txn['sector-code']),
            'sector': np.repeat(txn['sector'], len_of_split),
            'sector-percentage': chainer(txn['sector-percentage']),
            'default-aid-type-code': np.repeat(txn['default-aid-type-code'], len_of_split),
            'default-tied-status-code': np.repeat(txn['default-tied-status-code'], len_of_split) #todo: fillin all correct values for tied-status first
        })

        logger.info('exploded single rows with multiple values into multiple rows with single values in each row')

        # replace wrongly reported sector codes
        try:
            txn_df['sector-code'] = txn_df['sector-code'].astype(str).str.replace('15120', '15111')
            txn_df['sector-code'] = txn_df['sector-code'].astype(str).str.replace('23010', '23110')
            txn_df['sector-code'] = txn_df['sector-code'].astype(str).str.replace('23067', '23230')
            txn_df['sector-code'] = txn_df['sector-code'].astype(str).str.replace('23064', '23510')
            txn_df['sector-code'] = txn_df['sector-code'].astype(str).str.replace('23040', '23630')
            txn_df['sector-code'] = txn_df['sector-code'].astype(str).str.replace('23030', '23210')
            txn_df['sector-code'] = txn_df['sector-code'].astype(str).str.replace('73000', '15110')

            logger.info('replaced wrongly put sector-codes with the right ones')
        except Exception as e:
            logger.error(const.DOESNT_EXISTS, exc_info=True)

        # remove all characters from sector-codes
        txn_df['sector-code'].fillna(value=0, inplace=True)
        txn_df.loc[txn_df['sector-code'] == 'N/A', 'sector-code'] = 0
        txn_df.loc[txn_df['sector-code'] == 'nan', 'sector-code'] = 0
        txn_df['sector-code'] = pd.to_numeric(txn_df['sector-code']).astype(int)

        # remove all rows where sector-codes are not 5-digit or 7 digit codes.
        txn_df['sector-code'] = txn_df['sector-code'].astype(int)
        txn_df['sector-code'] = txn_df['sector-code'].apply(lambda x: rem_non_sectors(x))

        # remove all rows with sector-code as '' & nan
        txn_df = txn_df.loc[txn_df['sector-code'] != 0]
        txn_df = txn_df.loc[~txn_df['sector-code'].isna()]

        # mapping DAI sector definitions to activity
        txn_df['dai_sector'] = txn_df['sector-code'].apply(lambda x: dai_sectors_mapping(x, 'Level0', digits=5))
        txn_df['sector_category_code'] = txn_df['sector-code'].apply(lambda x: dai_sectors_mapping(x, 'Level1_code',
                                                                                                   digits=5))
        txn_df['sector_category'] = txn_df['sector-code'].apply(lambda x: dai_sectors_mapping(x, 'Level1', digits=5))
        txn_df['iati_sector_code'] = txn_df['sector-code'].apply(lambda x: dai_sectors_mapping(x, 'Level2_code',
                                                                                               digits=5))
        txn_df['iati_sector'] = txn_df['sector-code'].apply(lambda x: dai_sectors_mapping(x, 'Level2', digits=5))
        txn_df['subsector_code'] = txn_df['sector-code'].apply(lambda x: dai_sectors_mapping(x, 'Level3_code',
                                                                                             digits=7))
        txn_df['subsector'] = txn_df['sector-code'].apply(lambda x: dai_sectors_mapping(x, 'Level3', digits=7))

        # replace the NaN created due to non 5 or 7 digit sector codes with 'unknown' & 0
        txn_df['dai_sector'].fillna(value='unknown', inplace=True)
        txn_df['sector_category'].fillna(value='unknown', inplace=True)
        txn_df['iati_sector'].fillna(value='unknown', inplace=True)
        txn_df['subsector'].fillna(value='unknown', inplace=True)
        txn_df['sector_category_code'].fillna(value=0, inplace=True)
        txn_df['iati_sector_code'].fillna(value=0, inplace=True)
        txn_df['subsector_code'].fillna(value=0, inplace=True)

        logger.info("mapped DAC sectors to IATI's sector names")

        # fill NaN in sector & sector-percentage
        txn_df['sector'].fillna('unknown', inplace=True)
        txn_df.loc[txn_df['sector-percentage'].isna(), 'sector-percentage'] = 100

        # calc sector-wise transaction-amount
        txn_df['sector-txn-value'] = sector_disbursement(txn_df['sector-percentage'].astype(float),
                                                         txn_df['transaction-value'].astype(float))

        logger.info("calculated sector-wise transaction value from total transaction value")

        # clean reporting-org names
        txn_df['reporting-org'] = txn_df['reporting-org'].apply(lambda x: x.replace('¿', '-'))
        txn_df['default-aid-type-code'].fillna('Unknown', inplace=True)

        # clean country names
        txn_df['recipient-country'].replace(to_replace="Congo, The Democratic Republic Of The",
                                            value="Democratic Republic of the Congo", inplace=True)
        txn_df['recipient-country'].replace(to_replace="Cã\x94Te D'Ivoire",
                                            value="Côte d'Ivoire", inplace=True)
        txn_df['recipient-country'].replace(to_replace="Iran, Islamic Republic Of",
                                            value="Iran", inplace=True)
        txn_df['recipient-country'].replace(to_replace="Macedonia, The Former Yugoslav Republic Of",
                                            value="Former Yugoslav Republic of Macedonia", inplace=True)
        txn_df['recipient-country'].replace(to_replace="Micronesia, Federated States Of",
                                            value="Micronesia", inplace=True)
        txn_df['recipient-country'].replace(to_replace="Tanzania, United Republic Of",
                                            value="Tanzania", inplace=True)
        txn_df['recipient-country'].replace(to_replace="Saint Helena, Ascension And Tristan Da Cunha",
                                            value="Saint Helena", inplace=True)
        txn_df['recipient-country'].replace(to_replace="Venezuela, Bolivarian Republic Of",
                                            value="Venezuela", inplace=True)
        txn_df['recipient-country'].replace(to_replace="Korea, Democratic People'S Republic Of",
                                            value="Democratic People's Republic of Korea", inplace=True)
        txn_df['recipient-country'].replace(to_replace="Korea, Republic Of",
                                            value="Democratic People's Republic of Korea", inplace=True)
        txn_df['recipient-country'].replace(to_replace="Curaã\x87Ao",
                                            value="Curacao", inplace=True)

        # load DAI definition files
        dac_regions = pd.read_excel(cfg['PATH']['dai_region_def_file'])
        dac_regions.columns = ['recipient_code', 'recipient_name(en)', 'recipient_name(fr)', 'ISO_code', 'DAI_regions']

        # map country names to DAI region's definitions
        txn_df['recipient-country'] = txn_df['recipient-country'].str.strip().str.lower()
        dac_regions['recipient_name(en)'] = dac_regions['recipient_name(en)'].str.strip().str.lower()

        txn_df = txn_df.merge(dac_regions, how='left', left_on='recipient-country', right_on='recipient_name(en)')
        txn_df['recipient-country'] = txn_df['recipient-country'].apply(lambda x: camelcase_conversion(x))
        txn_df['recipient_name(en)'] = txn_df['recipient_name(en)'].apply(lambda x: camelcase_conversion(x))

        logger.info("correcting recipient-country name changes")

        # find start and end date for the activities
        # fill nan with the least possible date in pandas i.e. 1677-09-22
        txn_df['start-actual'] = txn_df['start-actual'].fillna(pd.Timestamp.min.ceil('D'))
        txn_df['start-planned'] = txn_df['start-planned'].fillna(pd.Timestamp.min.ceil('D'))
        txn_df['end-actual'] = txn_df['end-actual'].fillna(pd.Timestamp.min.ceil('D'))
        txn_df['end-planned'] = txn_df['end-planned'].fillna(pd.Timestamp.min.ceil('D'))

        # create start and end fields
        txn_df['start'] = np.where(
            txn_df['start-actual'].astype('datetime64[ns]') >= txn_df['start-planned'].astype('datetime64[ns]'),
            txn_df['start-actual'], txn_df['start-planned']
        )
        txn_df['end'] = np.where(
            txn_df['end-actual'].astype('datetime64[ns]') >= txn_df['end-planned'].astype('datetime64[ns]'),
            txn_df['end-actual'], txn_df['end-planned']
        )

        logger.info("calculated the start-date and end-date for the activities")

        # transaction-receiver-org as implementors
        # impute participating-org (Implementing) where transaction-receiver-org missing
        txn_df['implementor'] = txn_df['transaction_receiver-org'].fillna(
            value=txn_df['participating-org (Implementing)'])

        logger.info("mapped Organizations as Implementors")

        # timeline slicer for txn-year-month
        id_txn_yymm = txn_df.loc[~txn_df['transaction-date'].isna(), ['iati-identifier', 'transaction-date']]
        id_txn_yymm['txn-year'] = id_txn_yymm['transaction-date'].apply(lambda x: x.split('-')[0])
        id_txn_yymm['txn-month'] = id_txn_yymm['transaction-date'].apply(lambda x: x.split('-')[1])
        ids_and_years = id_txn_yymm[['iati-identifier', 'txn-year']]
        ids_and_years = ids_and_years.drop_duplicates()

        # write to csv --- return as dataframe
        # filename = cfg['PATH']['save_dir'] + 'activity_txn-year.xlsx'
        # ids_and_years.to_excel(filename, sheet_name='act_id_txn_year', index=False)

        logger.info("Processing completed successfully")

        return txn_df, ids_and_years
