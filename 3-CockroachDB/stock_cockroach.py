import psycopg2

'''
this project is store a sample stock market using cockroachdb
database name is (stockDB) and tables are:
stock: for saving stocks info(StockName,CompanyName,TotalShare,Price,EPS)
trader: for saving traders info(TraderId,TraderName)
trader_stock: for saving traders stocks info(TraderId,StockName,ShareCount)
sell_order: for saving sell order info(TraderId,StockName,ShareCount,Price,TradeDate,TradeTime)
buy_order: for saving buy order info(TraderId,StockName,ShareCount,Price,TradeDate,TradeTime)
'''

# make connection to db
connection = psycopg2.connect(database='stockdb', user='root', host='localhost', port=26257)
connection.set_session(autocommit=True)

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

    dict1 = {
        "Name": "SEB",
        "Company Name": "Seaboard Corp",
        "Total Share": 1161000,
        "Price": 3050.17,
        "EPS": 33.52
    }
    dict2 = {
        "Name": "BRK.A",
        "Company Name": "Berkshire Hathaway Inc",
        "Total Share": 1592000,
        "Price": 253500,
        "EPS": 3420
    }
    dict3 = {
        "Name": "NVR",
        "Company Name": "NVR INC",
        "Total Share": 3702000,
        "Price": 2795.06,
        "EPS": 208.46
    }

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

    dict1 = {
        "ID": "1111110",
        "Name": "Pooria Azizi",
        "Stocks": [
            {"Stock Name": "NVR", "Count": 150},
            {"Stock Name": "SEB", "Count": 1700},
            {"Stock Name": "BRK.A", "Count": 450}
        ]
    }
    dict2 = {
        "ID": "1111111",
        "Name": "Ali Molaee",
        "Stocks": [
            {"Stock Name": "BRK.A", "Count": 450},
            {"Stock Name": "NVR", "Count": 700}
        ]
    }
    dict3 = {
        "ID": "1111112",
        "Name": "Abbas Khalili",
        "Stocks": [
            {"Stock Name": "NVR", "Count": 900},
            {"Stock Name": "SEB", "Count": 400},
            {"Stock Name": "BRK.A", "Count": 57}
        ]
    }

    # adding sell order
    cursor.execute("INSERT INTO sell_order (TraderId, StockName, ShareCount, Price, TradeDate, TradeTime) VALUES "
                   "('1111112', 'SEB', '100', '3040', '2020/9/18', '21:15'),"
                   "('1111111', 'NVR', '300', '2700', '2020/9/18', '22:30'),"
                   "('1111110', 'BRK.A', '400', '253500', '2020/9/18', '09:55')")

    dict1 = {
        "ID": "1111112",
        "Stock": "SEB",
        "Count": 100,
        "Price": 3040,
        "Time": "21:15"
    }
    dict2 = {
        "ID": "1111111",
        "Stock": "NVR",
        "Count": 300,
        "Price": 2700,
        "Time": "22:30"
    }
    dict3 = {
        "ID": "1111110",
        "Stock": "BRK.A",
        "Count": 400,
        "Price": 253500,
        "Time": "09:55"
    }

    # adding buy order
    cursor.execute("INSERT INTO buy_order (TraderId, StockName, ShareCount, Price, TradeDate, TradeTime) VALUES "
                   "('1111112', 'NVR', '1000', '2700', '2020/9/18', '17:15'),"
                   "('1111111', 'BRK.A', '8800', '263500', '2020/9/18', '02:50'),"
                   "('1111110', 'SEB', '800', '3100', '2020/9/18', '14:45')")

    dict1 = {
        "ID": "1111110",
        "Stock": "SEB",
        "Count": 800,
        "Price": 3100,
        "Time": "14:45"
    }
    dict2 = {
        "ID": "1111112",
        "Stock": "NVR",
        "Count": 1000,
        "Price": 2700,
        "Time": "17:15"
    }
    dict3 = {
        "ID": "1111111",
        "Stock": "BRK.A",
        "Count": 8800,
        "Price": 263500,
        "Time": "02:50"
    }

def add_stock():
    StockName = input("Enter stock name: ")
    CompanyName = input("Enter company name: ")
    Price = input("Enter stock price: ")  # price of each share
    EPS = input("Enter stock EPS: ")  # earn per share
    TotalShare = input("Enter total number of shares for stock: ")  # number of all shares

    # insert the record to db
    cursor.execute("INSERT INTO stock (StockName, CompanyName, TotalShare, Price, EPS) VALUES "
                   "('" + StockName + "\',\'" + CompanyName + "\',\'" + TotalShare +
                   "\',\'" + Price + "\',\'" + EPS + "\')")

    print("Done.")

def search_stock():
    # receiving the stock details
    name = input("Enter the stock name: ")

    # fetch data from DB
    cursor.execute("SELECT StockName,CompanyName,TotalShare,Price,EPS"
                   " From stock WHERE StockName = '" + name + "\'")
    res = cursor.fetchall()
    fields = ["StockName", "CompanyName", "TotalShare", "Price", "EPS"]
    for row in res:
        f = 0
        for col in row:
            print(f"{fields[f]} : {col}")
            f = f + 1

def update_stock_info():
    # receiving stock name , total_share and price for update
    name = input("Enter stock name: ")
    total_share = input("Enter stock total number of shares: ")
    price = input("Enter stock new price: ")

    # update the record in db
    cursor.execute("UPDATE stock SET TotalShare =\'" + total_share + "\', Price =\'" + price +
                "\' WHERE  StockName=\'" + name + "\'")

    print("Done.")

def update_trader_name():
    # receiving name for update
    id = input("Enter trader ID: ")
    name = input("Enter trader new name: ")

    # update the record in db
    cursor.execute("UPDATE trader SET TraderName =\'" + name +
                   "\' WHERE  TraderId=\'" + id + "\'")

    print("Done.")

def add_sell_order():
    # receiving sell information
    id = input("Enter seller id: ")
    stock = input("Enter stock name: ")
    price = input("Enter stock price: ")
    count = input("Enter shares count: ")
    date = input("Enter sell date: ")
    time = input("Enter sell time: ")

    # insert the sell record to DB
    cursor.execute("INSERT INTO sell_order (TraderId,StockName,ShareCount,Price,TradeDate,TradeTime) VALUES "
                "(\'" + id + "\',\'" + stock + "\',\'" + count +
                "\',\'" + price +"\',\'" + date + "\',\'" + time + "\')")

    print("Done.")

def add_buy_order():
    # receiving buy information
    id = input("Enter buyer id: ")
    stock = input("Enter stock name: ")
    price = input("Enter stock price: ")
    count = input("Enter shares count: ")
    date = input("Enter buy date: ")
    time = input("Enter buy time: ")

    # insert the buy record to DB
    cursor.execute("INSERT INTO buy_order (TraderId,StockName,ShareCount,Price,TradeDate,TradeTime) VALUES "
                   "(\'" + id + "\',\'" + stock + "\',\'" + count +
                   "\',\'" + price + "\',\'" + date + "\',\'" + time + "\')")

    print("Done.")

def search_stocks_high_EPS():
    # fetch all data from db
    cursor.execute("SELECT StockName,EPS From stock")

    # filter the data with high EPS
    res = cursor.fetchall()
    stock = ""
    eps = ""
    for row in res:
        c = 0
        for col in row:
            if (c == 0):
                stock = col
            elif (c == 1):
                eps = col
            c = c + 1
        if(float(eps)>100):
            print(f"{stock} --> EPS: {eps}")

def search_trader_stocks():
    # receiving id to fetch stocks
    id = input("Enter trader ID: ")

    # fetch trader and its stocks from DB
    cursor.execute("SELECT trader.TraderId, trader.TraderName, trader_stock.StockName, trader_stock.ShareCount"
                   " From trader_stock INNER JOIN trader ON trader.TraderId = trader_stock.TraderId "
                   "WHERE trader_stock.TraderId = \'" + id + "\'")
    res = cursor.fetchall()

    # printing the stocks list
    flag = True
    name = ""
    stock = ""
    count = ""
    r = 1
    for row in res:
        c=0
        for col in row:
            if (c == 1):
                name = col
            elif(c==2):
                stock = col
            elif(c==3):
                count = col
            c=c+1

        if(flag):
            print(f"ID: {id} , Name: {name}")
            flag = False

        print(f"{r}) {stock} , Count: {count}")
        r = r+1

def search_trader_buy_orders():
    # receiving id to fetch stocks
    id = input("Enter trader ID: ")

    # fetch trader and its stocks from DB
    cursor.execute("SELECT trader.TraderId, trader.TraderName, buy_order.StockName, buy_order.ShareCount,"
                   "buy_order.Price, buy_order.TradeDate, buy_order.TradeTime"
                   " From buy_order INNER JOIN trader ON trader.TraderId = buy_order.TraderId"
                   " WHERE trader.TraderId = \'" + id + "\'")
    res = cursor.fetchall()

    # printing the stocks list
    flag = True
    name = ""
    stock = ""
    count = ""
    price = ""
    date = ""
    time = ""
    r = 1
    for row in res:
        c = 0
        for col in row:
            if (c == 1):
                name = col
            elif (c == 2):
                stock = col
            elif (c == 3):
                count = col
            elif (c == 4):
                price = col
            elif (c == 5):
                date = col
            elif (c == 6):
                time = col
            c = c + 1

        if (flag):
            print(f"ID: {id} , Name: {name}")
            flag = False

        print(f"{r}) {stock}, Count: {count}, Price: {price}, Date: {date}, Time: {time}")
        r = r + 1

# for the first time run tow lines below
create_db()
add_records_to_db()

while(True):
    print("what do you want to do?")
    print("1) Add a stock.")
    print("2) Search a stock.")
    print("3) Update stock info (Price - Total Share).")
    print("4) Update trader name.")
    print("5) Add sell order.")
    print("6) Add buy order.")
    print("7) Search for all stocks of a trader.")
    print("8) Search for stocks with the EPS>100.")
    print("9) Search buy orders of trader.")

    print("10) Exit.")

    job = input("Enter the number of your operation: ")
    print("----------------------------------------")

    if (int(job) == 10):
        break

    elif(int(job) == 1):
        add_stock()
    elif (int(job) == 2):
        search_stock()
    elif (int(job) == 3):
        update_stock_info()
    elif (int(job) == 4):
        update_trader_name()
    elif (int(job) == 5):
        add_sell_order()
    elif (int(job) == 6):
        add_buy_order()
    elif (int(job) == 8):
        search_stocks_high_EPS()
    elif (int(job) == 7):
        search_trader_stocks()
    elif (int(job) == 9):
        search_trader_buy_orders()

    print("----------------------------------------")