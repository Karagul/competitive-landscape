from datetime import datetime
from dateutil.relativedelta import relativedelta
import requests
from config import Config as cfg
from tqdm import tqdm
import sys
import pandas as pd
from itertools import chain
from misc.service_logger import serviceLogger as logger
import misc.constants as const

dai_sec = pd.read_csv(cfg['PATH']['dai_sector_def_file'])
dai_sec.columns = ['Hidden?', 'Column1', 'Level0', 'Level1_code', 'Level1', 'Level2_code', 'Level2', \
                   'Additional_notes', 'Level3_code', 'Level3']
# ---- fill nan in code-columns & change type to int
dai_sec['Level1_code'].fillna(0, inplace=True)
dai_sec['Level1_code'] = dai_sec['Level1_code'].astype(int)
dai_sec['Level2_code'].fillna(0, inplace=True)
dai_sec['Level2_code'] = dai_sec['Level2_code'].astype(int)

curr_exch = pd.read_csv(cfg['PATH']['currency_exchange_rates_file'])


def chainer(s):
    # return list from series of semi-colon separated strings
    return list(chain.from_iterable(s.str.split(';')))


def DownloadFile(filename, url):
    """
    to download all the files using urls
    """
    with requests.get(url, stream=True) as r:
        size = len(r.content)
        chunk_size = int(cfg['PATH']['chunk_size'])
        r.raise_for_status()
        with open(filename, 'wb') as f:
            for chunk in tqdm(iterable=r.iter_content(chunk_size=chunk_size), total=size/chunk_size, unit='KB'):
                if chunk:  # filtering out keep-alive new chunks
                    f.write(chunk)
                    # f.flush()
    return filename


def rem_non_sectors(row):
    """
    remove all digits that are not 5-digit or 7-digit codes
    """
    if isinstance(row, str):  # i.e. not a float (nan)
        if len(row) != 7:
            if len(row) != 5:
                row = ''
    return row


def dai_sectors_mapping(row, sector_level, digits=5):
    """
    row: df['col-with-sector-codes']
    sector_level : dai_sector_definition_df['col-with-dai-sector']
    digits: len of digits to take into consideration
    """
    if digits == 5:
        if (row != 0) & (len(str(row)) == 5):
            return dai_sec.at[dai_sec.loc[dai_sec['Level2_code'] == row].index.tolist()[0], sector_level]
    elif digits == 7:
        if (row != 0) & (len(str(row)) == 7):
            return dai_sec.at[dai_sec.loc[dai_sec['Level3_code'] == row].index.tolist()[0], sector_level]
    else:
        print("code currently not prepared for ", digits, "digits sector-codes!")
        logger.error(const.SECTOR_CODE_DOESNT_EXISTS, exc_info=True)
        sys.exit(0)


def sector_disbursement(sector_percentage, total_value):
    """
    make sure the col. containing sector-percentage and total disbursement values
    are type-casted and pass into the arguments as float-type
    """
    if sector_percentage is not None:
        return (sector_percentage / 100) * total_value
    else:
        return total_value


def camelcase_conversion(name):
    camelcase_str = []
    if isinstance(name, str):
        if len(name.split()) > 1:  # to exclude ABREVATIONS from getting converted to CamelCase string
            for char in name.split():
                camelcase_str.append(char.capitalize())
            return ' '.join(camelcase_str)
        else:
            return name.upper()  # return abrv. as all upper case


def currency_conv_USD(date_actual, date_planned, currency_code, value):
    """
    pass in the year and currency unit,
    it will  convert the 'value' into USD as per the conversion rate in that year of the 'date'
    """
    if isinstance(date_actual, str):  # date_sctual is not NaN
        # dates are in dd-mm-yyyy format
        year = date_actual.split('-')[0]
    elif (isinstance(date_actual, float)) and (isinstance(date_planned, str)):
        # date_actual is NaN but date_planned not NaN
        year = date_planned.split('-')[0]
    else:
        year = datetime.now().year  # both are NaN, take current year

    # for future years, consider current year
    if int(year) > datetime.now().year:
        year = datetime.now().year

    if value is None:
        value = 0

    exchange_value = curr_exch.loc[curr_exch['curr_codes'] == currency_code][year].values[0]
    return float(value) * exchange_value


def currency_conv_EUR(date_actual, date_planned, currency_code, value):
    """
    pass in the year and currency unit,
    it will  convert the 'value' into EUR as per the conversion rate in that year of the 'date'
    """
    if date_actual:
        # dates are in dd-mm-yyyy format
        year = int(date_actual.split('-')[0])
    elif (date_actual is None) and (date_planned is not None):
        year = int(date_planned.split('-')[0])
    else:
        year = int(datetime.now().year)

    eur_exchange_value = curr_exch.loc[curr_exch['year'] == year, 'EUR'].values[0]
    return float(value) * eur_exchange_value


def currency_conv_GBP(date, currency_code, value):
    """
    pass in the year and currency unit,
    it will  convert the 'value' into GBP as per the conversion rate in that year of the 'date'
    """
    # dates are in dd-mm-yyyy format
    year = int(date.split('-')[2])
    usd_exchange_value = curr_exch.loc[curr_exch['year'] == year, currency_code].values[0]
    usd_value = float(value) * usd_exchange_value
    gbp_exchange_value = curr_exch.loc[curr_exch['year'] == year, 'GBP'].values[0]
    return float(value) * gbp_exchange_value


def calc_no_of_yrs(start, end):
    if start > end:
        return start-end+1
    else:
        return end-start+1


def bucketing_multilaters(org_type):
    """
    bucket multilaterals ['participating-org-type (Implementing)'] into 6 buckets:
    - Bucket 1 (Universities)= "Academic, Training and Research"
    - Bucket 2 (Foundations) = "Foundation"
    - Bucket 3 (Govt./PPP) = "Government", "Other Public Sector", "Public Private Partnership"
    - Bucket 4 (NGOs) = "International NGO", "National NGO", "Regional NGO"
    - Bucket 5 (Multilaterals) – "Multilateral"
    - Bucket 6 (Private Sector) – "Private Sector"
    """
    if org_type == "Academic, Training and Research":
        return "Universities"
    elif org_type == "Foundation":
        return "Foundations"
    elif org_type == "Multilateral":
        return "Multilaterals"
    elif org_type == "Private Sector":
        return "Private Sectors"
    elif org_type in ["Government", "Other Public Sector", "Public Private Partnership"]:
        return "Government/PPP"
    elif org_type in ["International NGO", "National NGO", "Regional NGO"]:
        return "NGO"
    else:
        return "Unspecified"

def projects_ended(end_date):
    today = datetime.now()
    five_yrs_ago_date = today - relativedelta(years=5)
    five_yrs_from_now_date = today + relativedelta(years=5)
    if (datetime.strptime(end_date, "%Y-%m-%d") < today) \
    & (datetime.strptime(end_date, "%Y-%m-%d") >= five_yrs_ago_date):
        return 'in last 5 years'
    elif (datetime.strptime(end_date, "%Y-%m-%d") >= today):
        return 'still active'
    else:
        return 'earlier than 5 years'


def project_end_status(date_field):
    today = datetime.now()
    date_five_yrs_ago = today - relativedelta(years=5)
    # date_five_yrs_from_now = today + relativedelta(years=5)
    result = []
    if (datetime.strptime(date_field, "%Y-%m-%d") < today) \
    & (datetime.strptime(date_field, "%Y-%m-%d") >= date_five_yrs_ago): # end date between yesterday & 5 years from today
        result.append('in last 5 years')

    if (datetime.strptime(date_field, "%Y-%m-%d") >= today):  # end date is equal to today or greater than today
        result.append('still active')

    if (datetime.strptime(date_field,
                          "%Y-%m-%d") < date_five_yrs_ago):  # end date between -infinity to the date 5 years from today
        result.append('earlier than 5 years')
    return ';'.join(result)

def sector_percentage_splitter(num):
    result = []
    value = int(100/num)
    for ii in range(num):
        result.append(str(value))
    return ';'.join(result)