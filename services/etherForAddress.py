import time
from influxdb import InfluxDBClient
import requests


def write_to_influx():
    global metric, address, ether_balance, ethbtc, ethusd, address_txn_count, api_call, e, fields, tags, point, datapoints
    metric = 'wallet1'
    address = address
    Txn_hash = transaction['hash']
    timestamp = float(transaction['timeStamp'])
    blockNumber = transaction['blockNumber']
    _from = transaction['from']
    _to = transaction['to']
    isError = transaction['isError']
    value = transaction['value']
    txn_fee = int(transaction['gasUsed']) * int(transaction['gasPrice']) * 0.000000000000000001
    ether_balance = int(ether_balance)
    ethbtc = float(ether_price['ethbtc'])
    ethusd = float(ether_price['ethusd'])
    address_txn_count = address_txn_count
    try:
        api_call = 'https://api.etherscan.io/api?module=proxy&action=eth_blockNumber&apikey=' + api_key
        latest_block = requests.get(api_call).json()
    except Exception as e:
        print("error on med gas price request", e)
    latest_block = latest_block['result']

    fields = {
        "Txn_hash": Txn_hash,
        "blockNumber": float(blockNumber),
        # "blockReward": float(blockReward),
        # "uncleReward": float(uncleReward),
        "timestamp": timestamp,
        "address": address,
        "_from": _from,
        "_to": _to,
        "isError": float(isError),
        "value": float(value),
        "txn_fee": float(txn_fee),
        "address_txn_count": address_txn_count,
        "ether_balance": float(ether_balance),
        "ethbtc": float(ethbtc),
        "ethusd": float(ethusd),
        "latest_block": latest_block,
    }
    tags = {
        "address": str(address)
    }
    point = {"measurement": metric, "time": time.time_ns(), "fields": fields, "tags": tags}
    datapoints.append(point)

    if len(datapoints) % batchsize == 0:
        print('Inserting %d datapoints...' % (len(datapoints)))

        datapoints = []


while 1 == 1:

    try:
        client = InfluxDBClient(host='80.211.140.70', port='8086', username='ashen', password='1234', database='db1')
    except Exception as e:
        client = None
        print(e)

    # client.drop_database('db1')
    # client.create_database('db1')

    client.create_retention_policy('week_only', '10m', 2, database="db1", default=True)
    wallets = ['0x78aC091fc36d97EC7fC60352827B4A79641475DC', '0x7D48ABeA39EED4D60DD77c1a470b8f6D464b810E']
    api_key = 'CBMQ6J698ZEVZ3UJDBRYTTYAGHAA1TZDUH'
    datapoints = []

    # general data----------------------------------------------------------------------------
    ether_price_respond = {}
    market_cap_respond = {}
    med_gas_price_respond = {}
    gas_confirmation_time = {}

    try:
        api_call = 'https://api.etherscan.io/api?module=stats&action=ethprice&apikey=' + api_key
        ether_price_respond = requests.get(api_call).json()
    except Exception as e:
        ether_price_respond = {}
        print("error on price request", e)

    try:
        api_call = 'https://api.etherscan.io/api?module=stats&action=ethsupply&apikey=' + api_key
        market_cap_respond = requests.get(api_call).json()
    except Exception as e:
        market_cap_respond = {}
        print("error on market cap request", e)

    try:
        api_call = 'https://api.etherscan.io/api?module=gastracker&action=gasoracle&apikey=' + api_key
        med_gas_price_respond = requests.get(api_call).json()
    except Exception as e:
        med_gas_price_respond = {}
        print("error on med gas price request", e)

    try:
        api_call = 'https://api.etherscan.io/api?module=gastracker&action=gasestimate&gasprice=2000000000&apikey=' + api_key
        gas_confirmation_time = requests.get(api_call).json()
    except Exception as e:
        gas_confirmation_time = {}
        print("error on med gas price request", e)

    if ether_price_respond:
        ether_price = ether_price_respond["result"]
    else:
        ether_price = ether_price_respond

    if market_cap_respond:
        market_cap = market_cap_respond["result"]
    else:
        market_cap = market_cap_respond

    if med_gas_price_respond:
        med_gas_price = med_gas_price_respond['result']
    else:
        med_gas_price = med_gas_price_respond

    if gas_confirmation_time:
        gas_confirmation_time = gas_confirmation_time["result"]
    else:
        gas_confirmation_time = gas_confirmation_time

    metric = 'general'

    ethbtc = float(ether_price['ethbtc'])
    ethusd = float(ether_price['ethusd'])
    market_cap = int(market_cap) / 1000000000000000000
    med_gas_price_avg = float(med_gas_price['ProposeGasPrice'])

    fields = {
        "ethbtc": float(ethbtc),
        "ethusd": float(ethusd),
        "market_cap": float(market_cap),
        "med_gas_price_avg": float(med_gas_price_avg),
        "gas_confirmation_time": float(gas_confirmation_time)
    }

    tags = {}

    point = {"measurement": metric, "time": time.time_ns(), "fields": fields, "tags": tags}

    datapoints.append(point)

    print('Inserting General Data')
    response = None
    try:
        response = client.write_points(datapoints, time_precision='n')
    except Exception as e:
        print(e)

    if not response:
        print('Problem inserting General Data, exiting...')
        exit(1)

    print("Wrote General Data, response: %s" % response)

    datapoints = []

    # wallets transaction data----------------------------------------------------------------------------

    for address in wallets:
        ether_balance_respond = {}
        ether_transactions_respond = {}
        address_txn_count = {}

        try:
            api_call = 'https://api.etherscan.io/api?module=account&action=balance&address=' + address + '&tag=latest&apikey=' + api_key
            ether_balance_respond = requests.get(api_call).json()
        except Exception as e:
            ether_balance_respond = {}
            print("error on balance request", e)

        try:
            api_call = 'https://api.etherscan.io/api?module=account&action=txlist&address=' + address + '&startblock=0&endblock=99999999&sort=desc&apikey=' + api_key
            ether_transactions_respond = requests.get(api_call).json()
        except Exception as e:
            ether_transactions_respond = {}
            print("error on transaction request", e)

        try:
            api_call = 'https://api.etherscan.io/api?module=proxy&action=eth_getTransactionCount&address=' + address + '&tag=latest&apikey=' + api_key
            address_txn_count = requests.get(api_call).json()
        except Exception as e:
            address_txn_count = {}
            print("error on transaction count request", e)

        if address_txn_count:
            address_txn_count = address_txn_count['result']
        else:
            address_txn_count = address_txn_count

        if ether_balance_respond:
            ether_balance = ether_balance_respond["result"]
        else:
            ether_balance = ether_balance_respond

        if ether_transactions_respond:
            ether_transactions = ether_transactions_respond["result"]
        else:
            ether_transactions = ether_transactions_respond

        address_txn_count = str(address_txn_count)
        address_txn_count = int(address_txn_count, 16)

        batchsize = len(ether_transactions)
        # threads = []
        for transaction in ether_transactions:
            metric = 'wallet1'
            address = address
            Txn_hash = transaction['hash']
            timestamp = float(transaction['timeStamp'])
            blockNumber = transaction['blockNumber']
            _from = transaction['from']
            _to = transaction['to']
            isError = transaction['isError']
            value = transaction['value']
            txn_fee = int(transaction['gasUsed']) * int(transaction['gasPrice']) * 0.000000000000000001
            ether_balance = int(ether_balance)
            ethbtc = float(ether_price['ethbtc'])
            ethusd = float(ether_price['ethusd'])
            address_txn_count = float(address_txn_count)
            total_transactions = batchsize

            fields = {
                "Txn_hash": Txn_hash,
                "blockNumber": float(blockNumber),
                # "blockReward": float(blockReward),
                # "uncleReward": float(uncleReward),
                "timestamp": timestamp,
                "address": address,
                "_from": _from,
                "_to": _to,
                "isError": float(isError),
                "value": float(value),
                "txn_fee": float(txn_fee),
                "address_txn_count": float(address_txn_count),
                "ether_balance": float(ether_balance),
                "ethbtc": float(ethbtc),
                "ethusd": float(ethusd),
                "total_transactions": float(total_transactions),
                # "latest_block": latest_block,
            }
            tags = {
                "address": str(address)
            }
            point = {"measurement": metric, "time": time.time_ns(), "fields": fields, "tags": tags}
            datapoints.append(point)

            if len(datapoints) % batchsize == 0:
                print('Inserting %d datapoints...' % (len(datapoints)))

                try:
                    response = client.write_points(datapoints, time_precision='n')
                except Exception as e:
                    print(e)

                if not response:
                    print('Problem inserting points, exiting...')
                    exit(1)

                print("Wrote %d points, up to %s, response: %s" % (len(datapoints), timestamp, response))

                datapoints = []
