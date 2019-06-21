import pyodbc
from sqlalchemy import create_engine
import urllib.parse
from misc.service_logger import serviceLogger as logger

# connection string goes here
server = 'tcp:dai-dev-azure-sql1.database.windows.net,1433'
database = 'OIMT_CompetitiveLandscape'
username = 'xxxx'
password = 'xxxx'


params = urllib.parse.quote_plus('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';Uid='+username+';Pwd='+password)
cnxn_string = 'mssql+pyodbc:///?odbc_connect={}'.format(params)
engine = create_engine(cnxn_string)
engine.connect()
logger.info("Azure SQL Database Server Connection status: ... OK")



# from sqlalchemy.sql import text
# with engine.connect() as conn:
    # query = text("""SELECT * FROM dbo.iati_txn""")
    # res = conn.execute(query)
    # for row in res:
    #   print(row)

