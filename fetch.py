from config import Config
from misc.helper import DownloadFile
from misc.service_logger import serviceLogger as logger
import misc.constants as const
import pyodbc

base_url = "http://datastore.iatistandard.org/api/1/access/"

reporting_orgs = []
filtered_orgs_list = list(Config['IATI']['reporting_orgs'].split(','))

if len(filtered_orgs_list) > 1:
    for org in filtered_orgs_list:
        if filtered_orgs_list.index(org) != len(filtered_orgs_list)-1:
            reporting_orgs.append(org + "%7C")
        else:
            reporting_orgs.append(org)
else:
    reporting_orgs.append(filtered_orgs_list[0])

# 1. activity-url with no repeated rows
activity_url = base_url + "activity.csv?reporting-org=" + ''.join(reporting_orgs) + "&start-date__gt=" + \
               Config['IATI']['after_date'] + "&end-date__lt=" + Config['IATI']['before_date'] + \
               "&stream=True"

# 2. activity-url with multi-sector expansion
activity_by_sector_url = base_url + "activity/by_sector.csv?reporting-org=" + ''.join(reporting_orgs) + \
                         "&start-date__gt=" + Config['IATI']['after_date'] + "&end-date__lt=" + \
                         Config['IATI']['before_date'] + "&stream=True"

# 3. activity-url with multi-country expansion
activity_by_country_url = base_url + "activity/by_country.csv?reporting-org=" + ''.join(reporting_orgs) + \
                          "&start-date__gt=" + Config['IATI']['after_date'] + "&end-date__lt=" + \
                          Config['IATI']['before_date'] + "&stream=True"

# 4. transaction-url with no repeated rows
transaction_url = base_url + "transaction.csv?reporting-org=" + ''.join(reporting_orgs) + "&start-date__gt=" + \
                  Config['IATI']['after_date'] + "&end-date__lt=" + Config['IATI']['before_date'] + "&stream=True"

# 5. transaction-url with multi-sector expansion
transaction_by_sector_url = base_url + "transaction/by_sector.csv?reporting-org=" + ''.join(reporting_orgs) + \
                            "&start-date__gt=" + Config['IATI']['after_date'] + "&end-date__lt=" + \
                            Config['IATI']['before_date'] + "&stream=True"

# 6. transaction-url with multi-country expansion
transaction_by_country_url = base_url + "transaction/by_country.csv?reporting-org=" + ''.join(reporting_orgs) + \
                             "&start-date__gt=" + Config['IATI']['after_date'] + "&end-date__lt=" + \
                             Config['IATI']['before_date'] + "&stream=True"

url = [activity_url, activity_by_sector_url, activity_by_country_url, transaction_url, transaction_by_sector_url, \
       transaction_by_country_url]

file_name = ['activity.csv', 'activity_by_sector.csv', 'activity_by_country.csv', 'transaction.csv', \
             'transaction_by_sector.csv', 'transaction_by_country.csv']

files_to_download = dict(zip(file_name, url))

"""
downloading the files
"""
for file, url in files_to_download.items():
    file_name = Config['PATH']['download_dir'] + file
    try:
        DownloadFile(filename=file_name, url=url)
        log_msg = "Downloaded " + file + " successfully."
        logger.info(log_msg)
    except Exception as e:
        logger.error(const.INTERNAL_SERV_ERROR, exc_info=True)
        exit(1)

"""
push to DB
"""
# server = '52.172.141.161,1433'
# database = 'iati_db'
# username = 'SA'
# password = 'Mymarketintel1357!'
# cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+password+';Trusted_Connection=yes')
# cursor = cnxn.cursor()
#
# cursor.execute("SELECT @@version;")
# result = cursor.fetchone()
# print(result)
