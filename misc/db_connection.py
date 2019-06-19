import pyodbc

# paste your connection string here
server = 'tcp:dai-dev-azure-sql1.database.windows.net,1433'
database = 'OIMT_CompetitiveLandscape'
username = 'xxxxxxx'
password = 'xxxxxxx'

cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';Uid='+username+';Pwd='+password)
cursor = cnxn.cursor()


# Driver={ODBC Driver 13 for SQL Server};Server=tcp:dai-dev-azure-sql1.database.windows.net,1433;
# Database=OIMT_CompetitiveLandscape;Uid=daiadmin@dai-dev-azure-sql1;Pwd={your_password_here};
# Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;



