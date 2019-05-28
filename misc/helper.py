from datetime import datetime

import requests
from config import Config as cfg
from tqdm import tqdm
import sys
import pandas as pd
from misc.service_logger import serviceLogger as logger
import misc.constants as const

dai_sec = pd.read_csv(cfg['PATH']['dai_sector_def_file'])
curr_exch = pd.read_csv(cfg['PATH']['currency_exchange_rates_file'])

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
    elif (isinstance(date_actual, float)) and (isinstance(date_planned, str)):  # date_actual is NaN but date_planned not NaN
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
