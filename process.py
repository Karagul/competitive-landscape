import pandas as pd
import numpy as np
from config import Config as cfg
from misc.helper import rem_non_sectors, dai_sectors_mapping
from misc.helper import sector_disbursement, camelcase_conversion
from misc.service_logger import serviceLogger as logger
import misc.constants as const
from misc.helper import dai_sec, curr_exch
from misc.helper import currency_conv_USD

import warnings
warnings.filterwarnings('ignore')

# load all the files from dataset
act = pd.read_csv('dataset/raw_data/activity.csv')
act_by_sec = pd.read_csv('dataset/raw_data/activity_by_sector.csv')
act_by_cnt = pd.read_csv('dataset/raw_data/activity_by_country.csv')
txn = pd.read_csv('dataset/raw_data/transaction.csv')
txn_by_sec = pd.read_csv('dataset/raw_data/transaction_by_sector.csv')
txn_by_cnt = pd.read_csv('dataset/raw_data/transaction_by_country.csv')

logger.info('step 1 ... loaded files in raw dataset directory successfully.')

# load DAI definition files

oecd_names = pd.read_excel(cfg['PATH']['oecd_shortened_name_file'])
oecd_names.drop(['Unnamed: 3', 'Unnamed: 4'], axis=1, inplace=True)
oecd_names.columns = ['code', 'original_sector_names', 'shorter_sector_names']

dac_regions = pd.read_excel(cfg['PATH']['dai_region_def_file'])
dac_regions.columns = ['recipient_code','recipient_name(en)','recipient_name(fr)','ISO_code','DAI_regions']

parent_grp = pd.read_excel(cfg['PATH']['parent_group_def_file'])

logger.info('step 2 ... loaded DAI definitions in the directory successfully.')

# ---- cleaning column names in dai sectors
dai_sec.columns = ['Hidden?', 'Column1', 'Level0', 'Level1_code', 'Level1', 'Level2_code', 'Level2', \
                   'Additional_notes', 'Level3_code', 'Level3']

logger.info('step 3 ... correcting column names in dai sector definitions.')

# ---- fill nan in code-columns & change type to int
dai_sec['Level1_code'].fillna(0, inplace=True)
dai_sec['Level1_code'] = dai_sec['Level1_code'].astype(int)
dai_sec['Level2_code'].fillna(0, inplace=True)
dai_sec['Level2_code'] = dai_sec['Level2_code'].astype(int)

# ---- replace wrongly put sector-codes with the right sector-codes
try:
    act_by_sec['sector-code'] = act_by_sec['sector-code'].astype(str).str.replace('15120', '15111')
    act_by_sec['sector-code'] = act_by_sec['sector-code'].astype(str).str.replace('23010', '23110')
    act_by_sec['sector-code'] = act_by_sec['sector-code'].astype(str).str.replace('23067', '23230')
    act_by_sec['sector-code'] = act_by_sec['sector-code'].astype(str).str.replace('23064', '23510')
    act_by_sec['sector-code'] = act_by_sec['sector-code'].astype(str).str.replace('23040', '23630')
    act_by_sec['sector-code'] = act_by_sec['sector-code'].astype(str).str.replace('23030', '23210')
    act_by_sec['sector-code'] = act_by_sec['sector-code'].astype(str).str.replace('73000', '15110')

    logger.info('step 4 ... replaced wrong put sector-codes with the right ones.')
except Exception as e:
    logger.error(const.DOESNT_EXISTS, exc_info=True)

# ---- remove all characters from sector-code field
act_by_sec['sector-code'].fillna(value=0, inplace=True)
act_by_sec.loc[act_by_sec['sector-code']=='N/A', 'sector-code'] = 0
act_by_sec.loc[act_by_sec['sector-code']=='nan', 'sector-code'] = 0

# act_by_sec['sector-code'] = act_by_sec['sector-code'].str.replace(r'[^\d]+', '')
act_by_sec['sector-code'] = pd.to_numeric(act_by_sec['sector-code']).astype(int)

# ---- remove all rows where sector-codes are not 5-digit or 7 digit codes.
act_by_sec['sector-code'] = act_by_sec['sector-code'].astype(int)
act_by_sec['sector-code'] = act_by_sec['sector-code'].apply(lambda x: rem_non_sectors(x))

# ---- remove all rows with sector-code as '' & nan
act_by_sec = act_by_sec.loc[act_by_sec['sector-code'] != 0]
act_by_sec = act_by_sec.loc[~act_by_sec['sector-code'].isna()]

logger.info('step 5 ... filtered proper sector-codes and removed the improper ones.')

# ---- mapping DAI sector definitions to activity
act_by_sec['dai_sector'] = act_by_sec['sector-code'].apply(lambda x: \
                                                        dai_sectors_mapping(x, 'Level0', digits=5))
act_by_sec['sector_category_code'] = act_by_sec['sector-code'].apply(lambda x: \
                                                             dai_sectors_mapping(x, 'Level1_code', digits=5))
act_by_sec['sector_category'] = act_by_sec['sector-code'].apply(lambda x: \
                                                        dai_sectors_mapping(x, 'Level1', digits=5))
act_by_sec['subsector_code'] = act_by_sec['sector-code'].apply(lambda x: \
                                                             dai_sectors_mapping(x, 'Level2_code', digits=5))
act_by_sec['subsector'] = act_by_sec['sector-code'].apply(lambda x: \
                                                        dai_sectors_mapping(x, 'Level2', digits=5))
act_by_sec['sub_subsector_code'] = act_by_sec['sector-code'].apply(lambda x: \
                                                             dai_sectors_mapping(x, 'Level3_code', digits=7))
act_by_sec['sub_subsector'] = act_by_sec['sector-code'].apply(lambda x: \
                                                        dai_sectors_mapping(x, 'Level3', digits=7))

# ---- replace the NaN created due to non 5 or 7 digit sector codes with 'unknown' & 0
act_by_sec['dai_sector'].fillna(value='unknown', inplace=True)
act_by_sec['sector_category'].fillna(value='unknown', inplace=True)
act_by_sec['subsector'].fillna(value='unknown', inplace=True)
act_by_sec['sub_subsector'].fillna(value='unknown', inplace=True)

act_by_sec['sector_category_code'].fillna(value=0, inplace=True)
act_by_sec['subsector_code'].fillna(value=0, inplace=True)
act_by_sec['sub_subsector_code'].fillna(value=0, inplace=True)

logger.info("step 6 ... mapped DAI's sector definitions to the sector-codes in activities.")

# ---- replace any 0 (zero) in 'total-Disbursement' from the values in 'transaction-value' in txn_by_sec file
""" based on my hypothesis 2: posting vendors confused b/w Disbursement & Expenditure """
act_by_sec.loc[act_by_sec['total-Disbursement'] == 0, 'total-Disbursement'] = \
    txn_by_sec.loc[txn_by_sec['iati-identifier'].isin(
        list(act_by_sec.loc[act_by_sec['total-Disbursement'] == 0]['iati-identifier']))]['transaction-value']

logger.info("step 7 ... filled the missing values in 'total-Disbursement' field.")

# ---- fill nan in sector & sector-percentage
act_by_sec['sector'].fillna('unknown', inplace=True)
act_by_sec.loc[act_by_sec['sector-percentage'].isna(), 'sector-percentage'] = 100

# ---- distribute total-Disbursement into sector-wise disbursement
act_by_sec['sector-Disbursement'] = sector_disbursement(act_by_sec['sector-percentage'].astype(float), \
                                                 act_by_sec['total-Disbursement'].astype(float))

# ---- currency conversion
#merge default-currency to act_by_sec
act_by_sec = pd.merge(act_by_sec, act[['iati-identifier', 'default-currency']], how='left', on='iati-identifier')

# convert default-currency to USD
act_by_sec['Disbursement_USD'] = act_by_sec.apply(lambda x: \
                                                      currency_conv_USD(x['start-actual'], x['start-planned'],\
                                                                        x['default-currency'], x['sector-Disbursement']), axis=1)
# # convert USD value to EUR
# act_by_sec['Disbursement_EUR'] = act_by_sec.apply(lambda x: currency_conv_EUR(x['start-actual'],\
#                                                                               x['start-planned'], 'USD',\
#                                                                               x['Disbursement_USD']))
# # convert USD value to GBP
# act_by_sec['Disbursement_GBP'] = act_by_sec.apply(lambda x: currency_conv_GBP(x['start-actual'],\
#                                                                               x['start-planned'], 'USD',\
#                                                                               x['Disbursement_USD']))

logger.info("step 8 ... distributed total-Disbursement into sector wise Disbursements.")

# ---- map Implementors to Parent group Company names
parent_grp.columns = ['implementor_for_dfid_ec', 'exclude?', 'parent_group', 'in_crm?']

act_by_sec['participating-org (Implementing)'] = act_by_sec['participating-org (Implementing)'].str.strip().str.lower()
parent_grp['implementor_for_dfid_ec'] = parent_grp['implementor_for_dfid_ec'].str.strip().str.lower()

# merge act_by_sec & parent_grp
data = pd.merge(act_by_sec, parent_grp, how='left', left_on='participating-org (Implementing)', \
                        right_on='implementor_for_dfid_ec')

data.loc[data['parent_group'].isna(), 'parent_group'] = \
    data.loc[data['parent_group'].isna(), 'participating-org (Implementing)'].str.strip().str.lower()

data['parent_group'].fillna('unknown', inplace=True)
# convert names in lower case to CamelCase/PascalCase
data['parent_group'] = data['parent_group'].apply(lambda x: camelcase_conversion(x))

logger.info("step 9 ... mapped Implementors to Parent Group Companies.")

# change sector-codes to int types
data['sector-code'] = data['sector-code'].astype(int)

# ---- map sector-codes to OECD shorthand names
#merge data & orcd shorter names of sectors
final = pd.merge(data, oecd_names, how='left', left_on='sector-code', right_on='code')

logger.info("step 10 ... mapped OECD shortened names to sector names.")

# ---- drop unnecessary fields & write to excel
act.drop(labels=['participating-org (Accountable)', 'participating-org-ref (Accountable)', \
                      'participating-org-type (Accountable)', 'participating-org-type-code (Accountable)', \
                      'participating-org (Funding)', 'participating-org-ref (Funding)', \
                      'participating-org-type (Funding)', 'participating-org-type-code (Funding)', \
                      'participating-org (Extending)', 'participating-org-ref (Extending)', \
                      'participating-org-type (Extending)', 'participating-org-type-code (Extending)', \
                      'participating-org (Implementing)', 'participating-org-ref (Implementing)', \
                      'participating-org-type (Implementing)', 'participating-org-type-code (Implementing)', \
                      'recipient-country-code', 'recipient-country', 'recipient-country-percentage', \
                      'recipient-region-code', 'recipient-region', 'recipient-region-percentage', \
                      'sector-code', 'sector', 'sector-percentage', 'sector-vocabulary', \
                      'sector-vocabulary-code', 'collaboration-type-code', 'hierarchy', 'reporting-org-ref', \
                      'reporting-org-type','reporting-org-type-code'], axis=1, inplace=True)

act['reporting-org'] = act['reporting-org'].apply(lambda x: x.replace('¿', '-'))

act['default-tied-status-code'].fillna('5', inplace=True)
act['default-tied-status-code'] = act['default-tied-status-code'].str.lower()
act['default-tied-status-code'] = act['default-tied-status-code'].apply(lambda x: str(x).replace('nan', '5'))

act['default-tied-status-code'] = act['default-tied-status-code'].apply(lambda x: str(x).replace('untied', '5'))
act['default-tied-status-code'] = act['default-tied-status-code'].apply(lambda x: str(x).replace('tied', '4'))
act['default-tied-status-code'] = act['default-tied-status-code'].apply(lambda x: str(x).replace('partially tied', '3'))
act['default-tied-status-code'] = act['default-tied-status-code'].apply(lambda x: str(x).replace('u', '5'))
act['default-tied-status-code'] = act['default-tied-status-code'].apply(lambda x: str(x).replace('t', '4'))
# act['default-tied-status-code'] = act['default-tied-status-code'].apply(lambda x: str(x).replace('p', '3'))
act['default-tied-status-code'] = act['default-tied-status-code'].astype(int)
act['default-aid-type-code'].fillna('Unknown', inplace=True)

# ---- create yearly bins
# fill nan with least of datetime value in python
act['start-actual'].fillna('0001-01-01', inplace=True)
act['start-planned'].fillna('0001-01-01', inplace=True)
act['end-actual'].fillna('0001-01-01', inplace=True)
act['end-planned'].fillna('0001-01-01', inplace=True)
# grab just the year
act['start-actual-yr'] = act['start-actual'].apply(lambda x: x.split('-')[0])
act['start-planned-yr'] = act['start-planned'].apply(lambda x: x.split('-')[0])
act['end-actual-yr'] = act['end-actual'].apply(lambda x: x.split('-')[0])
act['end-planned-yr'] = act['end-planned'].apply(lambda x: x.split('-')[0])
# create a start and end field for the start year and end year
act['start'] = np.where(act['start-actual-yr'].astype(int) >= act['start-planned-yr'].astype(int), \
                       act['start-actual-yr'], act['start-planned-yr'])
act['end'] = np.where(act['end-actual-yr'].astype(int) >= act['end-planned-yr'].astype(int), \
                       act['end-actual-yr'], act['end-planned-yr'])
# drop the unnecessary intermediate fields
act.drop(labels=['start-actual-yr','start-planned-yr','end-actual-yr','end-planned-yr'], axis=1, inplace=True)
# find max year and min year, create df with columns = id, yr(min).....till yr(max) [expand in downward fashion]
ids_and_years = [(i, x) for i,a,b in act[['iati-identifier','start','end']].values for x in range(int(a),int(b)+1)]
id_timeline_df = pd.DataFrame(ids_and_years, columns=['ids','years'])

filename = cfg['PATH']['save_dir']+'id_timeline.xlsx'
id_timeline_df.to_excel(filename, sheet_name='id_years', index=False)

logger.info("setp 11 ... wrote iati-identifiers and their active years to disk.")

# create separate df with iati-identifier and the years falling in between start and end field
act_timeline = pd.DataFrame(columns=['iati-idnetifier','years_operational'])


filename = cfg['PATH']['save_dir']+'activity.xlsx'
act.to_excel(filename, sheet_name='activities', index=False)

logger.info("step 12 ... wrote activities to disk.")

final.drop(labels=['reporting-org-ref', 'reporting-org-type', 'reporting-org-type-code', \
                  'activity-status-code', 'recipient-country-code', 'recipient-country', \
                  'recipient-country-percentage', 'recipient-region-code', 'recipient-region', \
                  'recipient-region-percentage', 'default-aid-type-code', 'default-tied-status-code', \
                  'sector-vocabulary', 'sector-vocabulary-code', 'hierarchy', 'last-updated-datetime',
                  'default-language', 'description', 'participating-org (Accountable)', \
                  'participating-org-ref (Accountable)', 'participating-org-type (Accountable)', \
                  'participating-org-type-code (Accountable)', 'participating-org (Extending)', \
                  'participating-org-ref (Extending)', 'participating-org-type (Extending)', \
                  'participating-org-type-code (Extending)', 'collaboration-type-code', \
                  'default-finance-type-code', 'default-flow-type-code', \
                  'total-Incoming Funds', 'total-Interest Repayment', 'total-Loan Repayment', \
                  'total-Reimbursement'], axis=1, inplace=True)

filename = cfg['PATH']['save_dir']+'activities_by_sector.xlsx'
final.to_excel(filename, sheet_name='by_sector', index=False)

logger.info("step 13 ... wrote activities by sector to disk.")

# map activities by country to DAI country-region definitions

# some corrections in the country names before doing a lookup or merge
#TODO: let this be config-driven for future name correction, won't have to change anything in code

act_by_cnt['recipient-country'].replace(to_replace="Congo, The Democratic Republic Of The", \
                                        value="Democratic Republic of the Congo", inplace=True)
act_by_cnt['recipient-country'].replace(to_replace="Cã\x94Te D'Ivoire", value="Côte d'Ivoire", inplace=True)
act_by_cnt['recipient-country'].replace(to_replace="Iran, Islamic Republic Of", value="Iran", inplace=True)
act_by_cnt['recipient-country'].replace(to_replace="Macedonia, The Former Yugoslav Republic Of", \
                                        value="Former Yugoslav Republic of Macedonia", inplace=True)
act_by_cnt['recipient-country'].replace(to_replace="Micronesia, Federated States Of", value="Micronesia", inplace=True)
act_by_cnt['recipient-country'].replace(to_replace="Tanzania, United Republic Of", value="Tanzania", inplace=True)
act_by_cnt['recipient-country'].replace(to_replace="Saint Helena, Ascension And Tristan Da Cunha", \
                                        value="Saint Helena", inplace=True)
act_by_cnt['recipient-country'].replace(to_replace="Venezuela, Bolivarian Republic Of", value="Venezuela", inplace=True)
act_by_cnt['recipient-country'].replace(to_replace="Korea, Democratic People'S Republic Of", \
                                        value="Democratic People's Republic of Korea", inplace=True)
act_by_cnt['recipient-country'].replace(to_replace="Korea, Republic Of", \
                                        value="Democratic People's Republic of Korea", inplace=True)
act_by_cnt['recipient-country'].replace(to_replace="Curaã\x87Ao", value="Curacao", inplace=True)

logger.info("step 14 ... corrected some of the country names.")

# map country names to DAI region's definitions
act_by_cnt['recipient-country'] = act_by_cnt['recipient-country'].str.strip().str.lower()
dac_regions['recipient_name(en)'] = dac_regions['recipient_name(en)'].str.strip().str.lower()

c_data = act_by_cnt.merge(dac_regions, how='left', left_on='recipient-country', \
                        right_on='recipient_name(en)')

c_data.drop(labels=['hierarchy','default-language','reporting-org-ref', 'reporting-org-type', \
                    'reporting-org-type-code', 'activity-status-code', 'default-aid-type-code', \
                    'default-tied-status-code', 'sector-vocabulary', 'sector-vocabulary-code', 'hierarchy', \
                    'last-updated-datetime', 'default-language', 'description', 'participating-org (Accountable)', \
                    'participating-org-ref (Accountable)', 'participating-org-type (Accountable)', \
                    'participating-org-type-code (Accountable)', 'participating-org (Extending)', \
                    'participating-org-ref (Extending)', 'participating-org-type (Extending)', \
                    'participating-org-type-code (Extending)', 'collaboration-type-code', \
                    'default-finance-type-code', 'default-flow-type-code', 'total-Incoming Funds', \
                    'total-Interest Repayment', 'total-Loan Repayment', 'total-Reimbursement'], axis=1, inplace=True)

filename = cfg['PATH']['save_dir']+'activities_by_region.xlsx'
c_data.to_excel(filename, sheet_name='by_region', index=False)

logger.info("step 15 ... wrote activities by region to disk.")

logger.info(".... PROCESSING COMPLETED SUCCESSFULLY.")
