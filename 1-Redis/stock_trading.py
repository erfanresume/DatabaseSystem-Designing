import redis,json

# define and connect to redis db
Stock_DB = redis.Redis(host='localhost', port=6379, db=0)

# this method is for saving the low eps stocks into db
def indexing_low_eps(dict):
    # check the eps
    if(dict["EPS"] < 100 ):
        # fetching the all low eps stocks
        eps = str(Stock_DB.get("Low EPS Stocks"))
        eps = eps[2:-1]
        eps = json.loads(eps)
        # add the new stock to the list
        eps[dict["Name"]] = str(dict["EPS"])
        # set to db with (Low EPS Stocks) key
        Stock_DB.set("Low EPS Stocks", json.dumps(eps))

# this method is for adding some elemental data for program
def add_records_to_db():
    ### adding low eps key to db
    ### Stock_DB.set("Low EPS Stocks", '{"":""}')

    # adding some stocks
    dict = {
        "Name": "SEB",
        "Company Name": "Seaboard Corp",
        "Total Share": 1161000,
        "Price": 3050.17,
        "EPS": 33.52
    }

    # first entry of low eps stock most be manual to fill the json array
    Stock_DB.set("Low EPS Stocks", '{"SEB" : "33.52"}')
    Stock_DB.set("SEB", json.dumps(dict))

    dict = {
        "Name": "BRK.A",
        "Company Name": "Berkshire Hathaway Inc",
        "Total Share": 1592000,
        "Price": 253500,
        "EPS": 3420
    }
    indexing_low_eps(dict)
    Stock_DB.set("BRK.A", json.dumps(dict))

    dict = {
        "Name": "NVR",
        "Company Name": "NVR INC",
        "Total Share": 3702000,
        "Price": 2795.06,
        "EPS": 208.46
    }
    indexing_low_eps(dict)
    Stock_DB.set("NVR", json.dumps(dict))

    # adding some traders
    dict = {
        "ID": "1111110",
        "Name": "Pooria Azizi",
        "Stocks": [
            {"Stock Name" : "NVR" , "Count" : 150}
        ]
    }
    Stock_DB.set("1111110", json.dumps(dict))

    dict = {
        "ID": "1111111",
        "Name": "Ali Molaee",
        "Stocks": [
            {"Stock Name" : "BRK.A" , "Count" : 450},
            {"Stock Name": "NVR", "Count": 700}
        ]
    }
    Stock_DB.set("1111111", json.dumps(dict))

    dict = {
        "ID": "1111112",
        "Name": "Abbas Khalili",
        "Stocks": [
            {"Stock Name" : "SEB" , "Count" : 14000}
        ]
    }
    Stock_DB.set("1111112", json.dumps(dict))

    # adding sell order
    dict = {"Sell Orders" : [
            { "ID": "1111112" , "Stock": "SEB" , "Count": 400 , "Price": 3000 , "Time": "21:15" }
        ]
    }
    Stock_DB.set("sell", json.dumps(dict))

    # adding buy order
    dict = {"Buy Orders": [
        {"ID": "1111110", "Stock": "SEB", "Count": 100, "Price": 3100, "Time": "08:45"}
    ]
    }
    Stock_DB.set("buy", json.dumps(dict))

def add_stock():
    name = input("Enter stock name: ")
    company_name = input("Enter company name: ")
    price = input("Enter stock price: ") #price of each share
    eps = input("Enter stock EPS: ") #earn per share
    total_share = input("Enter stock total number of shares: ") #number of all shares

    # save the stock details in a dict
    stock_details = {
        "Name": name,
        "Company Name": company_name,
        "Total Share": int(total_share),
        "Price": float(price),
        "EPS": float(eps)
    }

    # check the eps
    indexing_low_eps(stock_details)

    # convert the stock details to json & store the json object in redis
    Stock_DB.set(name, json.dumps(stock_details))
    print("done")

def search_stock():
    name = input("Enter the stock Name: ")
    search_field = input("Enter the Search field:[Name,Company Name,Total Share,Price,EPS] ")

    stock_details = str(Stock_DB.get(name)) # read the stock details in json
    stock_details = stock_details[2:-1] # remove the (b') from the beginning of string

    # parsing json to dict
    stock_details_json = json.loads(stock_details)
    print(stock_details_json[search_field])

def update_trader_name():
    id = input("Enter trader ID: ")
    name = input("Enter trader new name: ")

    # fetching the person from db
    trader = str(Stock_DB.get(id))
    trader = trader[2:-1]

    # update the name
    trader_json = json.loads(trader)
    dict = {
        "ID": id,
        "Name": name,
        "Stocks":trader_json["Stocks"]
    }

    # store in db
    Stock_DB.set(id, json.dumps(dict))
    print("done")

def update_stock_info():
    name = input("Enter stock name: ")
    total_share = input("Enter stock total number of shares: ")
    price = input("Enter stock price: ")

    # fetching the stock from db
    stock = str(Stock_DB.get(name))
    stock = stock[2:-1]

    # update the stock --> price & total share
    stock = json.loads(stock)
    dict = {
        "Name": name,
        "Company Name": stock["Company Name"],
        "Total Share": int(total_share),
        "Price": float(price),
        "EPS": float(stock["EPS"])
    }

    # store in db
    Stock_DB.set(name, json.dumps(dict))
    print("done")

def add_sell_order():
    id = input("Enter seller id: ")
    stock = input("Enter stock name: ")
    price = input("Enter stock price: ")
    count = input("Enter shares count: ")
    time = input("Enter sell time: ")

    # save the sell order details in a dict
    new_sell_dict = {
        "ID": id,
        "Stock": stock,
        "Count": int(count),
        "Price": float(price),
        "Time": time
    }

    # load the json array of sell orders from redis and parse it to dict
    sell = str(Stock_DB.get("sell"))
    sell = sell[2:-1]
    sell = json.loads(sell)

    # add the new order to dict
    sell["Sell Orders"].append(new_sell_dict)

    # set to db
    Stock_DB.set("sell", json.dumps(sell))
    print("done")

def add_buy_order():
    id = input("Enter buyer id: ")
    stock = input("Enter stock name: ")
    price = input("Enter stock price: ")
    count = input("Enter shares count: ")
    time = input("Enter buy time: ")

    # save the buy order details in a dict
    new_buy_dict = {
        "ID": id,
        "Stock": stock,
        "Count": int(count),
        "Price": float(price),
        "Time": time
    }

    # load the json array of buy orders from redis and parse it to dict
    buy = str(Stock_DB.get("buy"))
    buy = buy[2:-1]
    buy = json.loads(buy)

    # add the new order to dict
    buy["Buy Orders"].append(new_buy_dict)

    # set to db
    Stock_DB.set("buy", json.dumps(buy))
    print("done")

def search_stocks_low_EPS():
    # read the stocks with low eps in json from db
    stock = str(Stock_DB.get("Low EPS Stocks"))
    # remove the (b') from the beginning of string
    stock = stock[2:-1]

    # parsing json to dict
    stock = json.loads(stock)
    for s in stock:
        print(s + " --> EPS = " + stock[s])

def search_sell_orders():
    sell = str(Stock_DB.get("sell"))
    sell = sell[2:-1]

    # parsing json to dict
    sell = json.loads(sell)
    for s in sell["Sell Orders"]:
        print(s)

add_records_to_db()

while(True):
    print("what do you want to do?")
    print("1) Add a stock.")
    print("2) Search a stock.")
    print("3) Update stock info.")
    print("4) Update trader name.")
    print("5) Add sell order.")
    print("6) Add buy order.")
    print("7) Search for all sell orders.")
    print("8) Search for stocks with the EPS<100.")

    print("9) Exit.")

    job = input("Enter the number of your operation: ")
    print("----------------------------------------")

    if (int(job) == 9):
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
        search_stocks_low_EPS()
    elif (int(job) == 7):
        search_sell_orders()

    print("----------------------------------------")