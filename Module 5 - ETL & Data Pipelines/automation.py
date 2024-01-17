# Import libraries required for connecting to mysql
import mysql.connector
# Import libraries required for connecting to DB2 or PostgreSql
import ibm_db
# IMport library for timestamping
import datetime

# Connect to MySQL
mysqlpw = ''
mysql_table = 'sales_data'
mysql_db = 'sales'
try:
    mysql_conn = mysql.connector.connect(user='root',password=mysqlpw,host='127.0.0.1',database=mysql_db)
    print(f"Connected to MySQL database ({mysql_db})")
except:
    print(f"Could not connect to MySQL database ({mysql_db})")
mysql_cursor = mysql_conn.cursor()

# Connect to DB2
dsn_hostname = "2f3279a5-73d1-4859-88f0-a6c3e6b4b907.c3n41cmd0nqnrk39u98g.databases.appdomain.cloud" # e.g.: "dashdb-txn-sbox-yp-dal09-04.services.dal.bluemix.net"
dsn_uid = ""        # e.g. "abc12345"
dsn_pwd = ""      # e.g. "7dBZ3wWt9XN6$o0J"
dsn_port = ""                # e.g. "50000" 
dsn_database = ""            # i.e. "BLUDB"
dsn_driver = "{IBM DB2 ODBC DRIVER}" # i.e. "{IBM DB2 ODBC DRIVER}"           
dsn_protocol = "TCPIP"            # i.e. "TCPIP"
dsn_security = "SSL"              # i.e. "SSL"
dsn = (
    "DRIVER={0};"
    "DATABASE={1};"
    "HOSTNAME={2};"
    "PORT={3};"
    "PROTOCOL={4};"
    "UID={5};"
    "PWD={6};"
    "SECURITY={7};").format(dsn_driver, dsn_database, dsn_hostname, dsn_port, dsn_protocol, dsn_uid, dsn_pwd, dsn_security)
ibm_table = 'sales_data'
try:
    ibm_conn = ibm_db.connect(dsn, "", "")
    print ("Connected to IBM DB2 database\n") #: ", dsn_database, "as user: ", dsn_uid, "on host: ", dsn_hostname)
except:
    print("Could not connect to IBM DB2\n")

# Find out the last rowid from DB2 data warehouse
# The function get_last_rowid must return the last 
# rowid of the table sales_data on the IBM DB2 database
def get_last_rowid(conn,table):
    SQL=f"SELECT rowid FROM {table} ORDER BY rowid DESC LIMIT 1"
    stmt = ibm_db.exec_immediate(conn, SQL)
    result = ibm_db.fetch_tuple(stmt)[0]
    return result

last_row_id = get_last_rowid(ibm_conn,ibm_table)
print("Last row id on production datawarehouse:", last_row_id)

# List out all records in MySQL database with rowid greater
# than the one on the Data warehouse
# The function get_latest_records must return a list of all 
# records that have a rowid greater than the last_row_id in the 
# sales_data table in the sales database on the MySQL staging 
# data warehouse.
def get_latest_records(rowid,cursor,table):
    SQL = f"SELECT * FROM {table} WHERE rowid > {rowid}"
    cursor.execute(SQL)
    result_list = []
    for row in cursor.fetchall():
	    result_list.append(list(row))
    return result_list
	
new_records = get_latest_records(last_row_id,mysql_cursor,mysql_table)
print("Number of new rows on staging datawarehouse = ", len(new_records))

# Insert the additional records from MySQL into DB2 data warehouse.
# The function insert_records must insert all the records 
# passed to it into the sales_data table in IBM DB2 database.
def insert_records(conn,new_records,table):
    timestamp = datetime.datetime.now().timestamp()

    for i in range(len(new_records)):
        rowid = new_records[i][0]
        product_id = new_records[i][1]
        customer_id = new_records[i][2]
        quantity = new_records[i][3]
        SQL_query = f"SELECT price FROM {table} WHERE product_id={product_id} LIMIT 1"
        stmt = ibm_db.exec_immediate(conn, SQL_query)
        try:
            price = ibm_db.fetch_tuple(stmt)[0]
        except:
            price = 0
        row = (rowid,product_id,customer_id,price,quantity,timestamp)
        SQL_insert = f"INSERT INTO {table} (rowid,product_id,customer_id,price,quantity,timestamp)\
              VALUES(?,?,?,?,?,?);"
        stmt = ibm_db.prepare(conn, SQL_insert)
        ibm_db.execute(stmt, row)

insert_records(ibm_conn,new_records,ibm_table)
print("New rows inserted into production datawarehouse = ", len(new_records))

print("Closing connections")
# disconnect from mysql warehouse
mysql_conn.close()
# disconnect from DB2 or PostgreSql data warehouse 
ibm_db.close(ibm_conn)
# End of program
print("Connections closed")