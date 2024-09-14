import psycopg2
from psycopg2 import extensions

# make connection to db
try:
    connection = psycopg2.connect(database='stockdb', user='root', host='localhost', port=26257)

    # setting transaction isolation level to serializable
    connection.set_isolation_level(extensions.ISOLATION_LEVEL_SERIALIZABLE)

    connection.set_session(autocommit=False)

except Exception as e:
    print(e)

else:
    # create a cursor for sending query to db
    cursor = connection.cursor()


# first initials of the db(create db and tables)
def create_db():
    # creating the database
    cursor.execute("DROP DATABASE IF EXISTS stockDB")
    cursor.execute("CREATE DATABASE IF NOT EXISTS stockDB")

    # creating database tables()
    # stock table
    cursor.execute("CREATE TABLE IF NOT EXISTS stock ("
                   "StockName varchar(255),"
                   "CompanyName varchar(255),"
                   "TotalShare varchar(255),"
                   "Price varchar(255),"
                   "EPS varchar(255),"
                   "PRIMARY KEY (StockName)"
                   ")")

    # trader table
    cursor.execute("CREATE TABLE IF NOT EXISTS trader ("
                   "TraderId varchar(255),"
                   "TraderName varchar(255),"
                   "PRIMARY KEY (TraderId)"
                   ")")

    # trader_stocks table
    cursor.execute("CREATE TABLE IF NOT EXISTS trader_stock ("
                   "TraderId varchar(255),"
                   "StockName varchar(255),"
                   "ShareCount varchar(255),"
                   "FOREIGN KEY (TraderId) REFERENCES trader(TraderId),"
                   "FOREIGN KEY (StockName) REFERENCES stock(StockName)"
                   ")")

    # sell_order table
    cursor.execute("CREATE TABLE IF NOT EXISTS sell_order ("
                   "TraderId varchar(255),"
                   "StockName varchar(255),"
                   "ShareCount varchar(255),"
                   "Price varchar(255),"
                   "TradeDate varchar(255),"
                   "TradeTime varchar(255),"
                   "FOREIGN KEY (TraderId) REFERENCES trader(TraderId),"
                   "FOREIGN KEY (StockName) REFERENCES stock(StockName)"
                   ")")

    # sell_order table
    cursor.execute("CREATE TABLE IF NOT EXISTS buy_order ("
                   "TraderId varchar(255),"
                   "StockName varchar(255),"
                   "ShareCount varchar(255),"
                   "Price varchar(255),"
                   "TradeDate varchar(255),"
                   "TradeTime varchar(255),"
                   "FOREIGN KEY (TraderId) REFERENCES trader(TraderId),"
                   "FOREIGN KEY (StockName) REFERENCES stock(StockName)"
                   ")")

# insert some data to db
def add_records_to_db():
    # adding some stocks
    cursor.execute("INSERT INTO stock (StockName, CompanyName, TotalShare, Price, EPS) VALUES "
                   "('SEB', 'Seaboard Corp', '1161000', '3050.17', '33.52'),"
                   "('BRK.A', 'Berkshire Hathaway Inc', '1592000', '253500', '3420'),"
                   "('NVR', 'NVR INC', '3702000', '2795.06', '208.46')")

    # adding some traders
    cursor.execute("INSERT INTO trader (TraderId, TraderName) VALUES "
                   "('1111110', 'Pooria Azizi'),"
                   "('1111111', 'Ali Molaee'),"
                   "('1111112', 'Abbas Khalili')")

    # adding trader's stocks
    cursor.execute("INSERT INTO trader_stock (TraderId, StockName, ShareCount) VALUES "
                   "('1111110', 'NVR', '150'),"
                   "('1111110', 'SEB', '1700'),"
                   "('1111110', 'BRK.A', '450'),"
                   "('1111111', 'NVR', '700'),"  # next
                   "('1111111', 'BRK.A', '450'),"
                   "('1111112', 'NVR', '900'),"  # next
                   "('1111112', 'SEB', '400'),"
                   "('1111112', 'BRK.A', '57')")

    # adding sell order
    cursor.execute("INSERT INTO sell_order (TraderId, StockName, ShareCount, Price, TradeDate, TradeTime) VALUES "
                   "('1111112', 'SEB', '100', '3040', '2020/9/18', '21:15'),"
                   "('1111111', 'NVR', '300', '2700', '2020/9/18', '22:30'),"
                   "('1111110', 'BRK.A', '400', '253500', '2020/9/18', '09:55')")


    # adding buy order
    cursor.execute("INSERT INTO buy_order (TraderId, StockName, ShareCount, Price, TradeDate, TradeTime) VALUES "
                   "('1111112', 'NVR', '1000', '2700', '2020/9/18', '17:15'),"
                   "('1111111', 'BRK.A', '8800', '263500', '2020/9/18', '02:50'),"
                   "('1111110', 'SEB', '800', '3100', '2020/9/18', '14:45')")


# executing the transactions
def tran1():
    try:
        cursor.execute("INSERT INTO stock (StockName, CompanyName, TotalShare, Price, EPS) VALUES "
                       "('Akhaber','Mokhaberat Iran','18000000','13.66','239')")

        cursor.execute("UPDATE stock SET TotalShare ='17000000' WHERE  StockName='Akhaber'")

        cursor.execute("SELECT * From stock WHERE  StockName='Akhaber'")
        res = cursor.fetchall()
        print(res)

    except psycopg2.DatabaseError as error:
        print(error)
        connection.rollback()
    else:
        connection.commit()
        print("Done.")

def tran2():
    try:
        cursor.execute("UPDATE trader SET Tradername ='Ehsan Anvari' WHERE  Traderid='1111112'")

        cursor.execute("INSERT INTO trader_stock (Traderid, Stockname, Sharecount) VALUES "
                       "('1111112','Akhaber','100000')")

        cursor.execute("DELETE FROM trader_stock WHERE Traderid ='1111112' AND Stockname = 'SEB'")


    except psycopg2.DatabaseError as error:
        print(error)
        connection.rollback()
    else:
        connection.commit()
        print("Done.")

def tran3():
    try:
        cursor.execute("DELETE FROM sell_order WHERE Traderid ='1111112'")

        cursor.execute("INSERT INTO sell_order (TraderId,StockName,ShareCount,Price,TradeDate,TradeTime) VALUES "
                       "('1111112','SEB','100','700','2021/1/1','00:00'),"
                       "('1111112','Akhaber','7500','145','2021/1/1','14:26')")

    except psycopg2.DatabaseError as error:
        print(error)
        connection.rollback()
    else:
        connection.commit()
        print("Done.")

def tran4():
    try:
        cursor.execute("INSERT INTO trader (TraderName, Traderid) VALUES "
                       "('Saman Saberi','2222245')")

        cursor.execute("INSERT INTO buy_order (TraderId,StockName,ShareCount,Price,TradeDate,TradeTime) VALUES "
                       "('2222245','SEB','5000','7541','2021/1/1','19:15')")

        cursor.execute("SELECT trader.TraderId, trader.TraderName, trader_stock.StockName, trader_stock.ShareCount "
                       "From trader_stock INNER JOIN trader ON trader.TraderId = trader_stock.TraderId "
                       "WHERE trader_stock.TraderId = '1111112'")
        res = cursor.fetchall()
        print(res)

    except psycopg2.DatabaseError as error:
        print(error)
        connection.rollback()
    else:
        connection.commit()
        print("Done.")


# for the first time run tow lines below and set auto admit to True in line 11
#create_db()
#add_records_to_db()

while(True):
    print("Choose Transaction number?")
    print("1) [Add a stock(akhaber)] [Update Total share] [Show new stock details].")
    print("2) [Update a trader name] [Add new share for trader] [Remove trader stock].")
    print("3) [Delete traders sells orders] [Add new sell orders].")
    print("4) [Add trader] [Add new buy order] [Show trader stocks].")

    print("5) Exit.")

    job = input("Enter the number of your Transaction: ")
    print("----------------------------------------")

    if (int(job) == 5):
        break

    elif(int(job) == 1):
        tran1()
    elif (int(job) == 2):
        tran2()
    elif (int(job) == 3):
        tran3()
    elif (int(job) == 4):
        tran4()
    else:
        continue

    print("----------------------------------------")