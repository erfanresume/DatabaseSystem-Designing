import pymongo

# make connection to db and declaration the collections
client = pymongo.MongoClient("localhost", 27017)
stock_db = client["stock_db"]
stock_col = stock_db["stock"]
trader_col = stock_db["trader"]
sell_col = stock_db["sell_order"]
buy_col = stock_db["buy_order"]
test = stock_db["test"]

def create_index_for_db():
    try:
        # create index for stock (Name) to be unique
        stock_col.create_index([('Name', pymongo.ASCENDING)], unique=True)

        # create index for trader (ID) to be unique
        trader_col.create_index([('ID', pymongo.ASCENDING)], unique=True)

        # create index for (ID) & (Stock) & (Time) to be unique in sell and buy collections
        sell_col.create_index([('ID', pymongo.ASCENDING),
                           ("Stock",pymongo.ASCENDING),
                           ("Time",pymongo.ASCENDING)],unique = True)

        # create index for (ID) & (Stock) & (Time) to be unique in sell and buy collections
        buy_col.create_index([('ID', pymongo.ASCENDING),
                               ("Stock", pymongo.ASCENDING),
                               ("Time", pymongo.ASCENDING)], unique=True)
    except:
        print("DataBase is unavailable")

# this method is for adding some elemental data for program to data base
def add_records_to_db():
    # adding some stocks
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
    try:
        stock_col.insert_many([dict1,dict2,dict3])
    except:
        print("Duplicate Key")

    # adding some traders
    dict1 = {
        "ID": "1111110",
        "Name": "Pooria Azizi",
        "Stocks": [
            {"Stock Name" : "NVR" , "Count" : 150},
            {"Stock Name": "SEB", "Count": 1700},
            {"Stock Name": "BRK.A", "Count": 450}
        ]
    }

    dict2 = {
        "ID": "1111111",
        "Name": "Ali Molaee",
        "Stocks": [
            {"Stock Name" : "BRK.A" , "Count" : 450},
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
    try:
        trader_col.insert_many([dict1,dict2,dict3])
    except:
        print("Duplicate Key")

    # adding sell order
    dict1 = {
        "ID": "1111112" ,
        "Stock": "SEB" ,
        "Count": 100 ,
        "Price": 3040 ,
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
    try:
        sell_col.insert_many([dict1,dict2,dict3])
    except:
        print("Duplicate Key")

    # adding buy order
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
    try:
        buy_col.insert_many([dict1, dict2, dict3])
    except:
        print("Duplicate Key")

def add_stock():
    name = input("Enter stock name: ")
    company_name = input("Enter company name: ")
    price = input("Enter stock price (Float): ")  # price of each share
    eps = input("Enter stock EPS (Float): ")  # earn per share
    total_share = input("Enter total number of shares for stock (Integer): ")  # number of all shares

    # save the stock details in a dict
    stock_details = {
        "Name": name,
        "Company Name": company_name,
        "Total Share": int(total_share),
        "Price": float(price),
        "EPS": float(eps)
    }

    # insert the record to db
    try:
        stock_col.insert_one(stock_details)
        print("Done")
    except:
        print("Duplicate Stock Name")

def search_stock():
    # receiving the stock details
    name = input("Enter the stock name: ")
    search_field = input("Enter the Search field:[Name,Company Name,Total Share,Price,EPS] ")

    # fetch data from DB
    try:
        stock = stock_col.find_one({"Name": name})
    except:
        stock = None

    if (stock == None):
        print("No such stock in DB")
    else:
        print(stock[search_field])

def update_stock_info():
    # receiving stock name , total_share and price for update
    name = input("Enter stock name: ")
    total_share = input("Enter stock total number of shares (Integer): ")
    price = input("Enter stock price (Float): ")

    # update the record in db
    try:
        stock_col.update_one({"Name":name},{"$set":{"Total Share": int(total_share), "Price": float(price)}})
        print("Done")
    except:
        print("Cannot be updated")

def update_trader_name():
    # receiving name for update
    id = input("Enter trader ID: ")
    name = input("Enter trader new name: ")

    # update the record in db
    try:
        trader_col.update_one({"ID": id}, {"$set": {"Name": name}})
        print("Done")
    except:
        print("Cannot be updated")

def add_sell_order():
    # receiving sell information
    id = input("Enter seller id: ")
    stock = input("Enter stock name: ")
    price = input("Enter stock price: ")
    count = input("Enter shares count: ")
    time = input("Enter sell time: ")

    # save the sell order details in a dict
    sell_order = {
        "ID": id,
        "Stock": stock,
        "Count": int(count),
        "Price": float(price),
        "Time": time
    }

    # insert the sell record to DB
    try:
        sell_col.insert_one(sell_order)
        print("Done")
    except:
        print("Duplicate Sell Order")

def add_buy_order():
    # receiving buy information
    id = input("Enter buyer id: ")
    stock = input("Enter stock name: ")
    price = input("Enter stock price: ")
    count = input("Enter shares count: ")
    time = input("Enter buy time: ")

    # save the buy order details in a dict
    buy_order = {
        "ID": id,
        "Stock": stock,
        "Count": int(count),
        "Price": float(price),
        "Time": time
    }
    new_stock={
        "Stock Name": stock,
        "Count": int(count)
    }

    # update the trader stocks list
    try:
        trader = trader_col.find_one({"ID": id})
        stocks = trader["Stocks"]
        stocks.append(new_stock)
        trader_col.update_one({"ID": id}, {"$set": {"Stocks": stocks}})
    except:
        print("cant be updated")
        return

    # insert the buy record to DB
    try:
        buy_col.insert_one(buy_order)
        print("Done")
    except:
        print("Duplicate Buy Order")

def search_stocks_high_EPS():
    # fetch all the stock in DB
    try:
        stocks = stock_col.find()
    except:
        stocks = None

    # filter and print the stocks with EPS>100
    for stock in stocks:
        if(stock["EPS"]>100):
            print(f"{stock['Name']} --> (Price:{stock['Price']} $) (EPS:{stock['EPS']})")

def search_trader_stocks():
    # receiving id to fetch stocks
    id = input("Enter trader ID: ")

    # fetch trader from DB
    try:
        trader = trader_col.find_one({"ID": id})
    except:
        trader = None

    if (trader == None):
        print("There is no such trader")
        return

    # printing the stocks list
    stocks = trader["Stocks"]
    print(f'{trader["Name"]} ({trader["ID"]}):')
    c=1
    for stock in stocks:
        print(f'\t{c}) {stock["Stock Name"]} --> Count:{stock["Count"]}')
        c=c+1

def search_trader_buy_orders():
    # receiving id to fetch buy orders
    id = input("Enter trader ID: ")

    # fetch orders from DB
    try:
        orders = buy_col.find({"ID": id})
        trader = trader_col.find_one({"ID": id})
    except:
        orders = None

    if (orders == None):
        print("There is no record")
        return

    # printing the buy order list
    print(f'{trader["Name"]} ({trader["ID"]}):')
    c=1
    for order in orders:
        print(f'\t{c}) {order["Stock"]} --> Count:{order["Count"]} , Price:{order["Price"]} , Time:{order["Time"]}')
        c=c+1

# if you have no sample data in your DB or in first run you need to uncomment line code below
#add_records_to_db() # only run this one time

#create_index_for_db()

while(True):
    print("what do you want to do?")
    print("1) Add a stock.")
    print("2) Search a stock.")
    print("3) Update stock info.")
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
