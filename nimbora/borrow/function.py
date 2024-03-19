import asyncio
import json
from datetime import datetime
import pandas as pd
import os
from web3 import Web3

token_manager_abi = json.load(open('./nimbora/borrow/TroveManager.abi.json'))
price_feed_abi = json.load(open('./nimbora/borrow/PriceFeed.abi.json'))

trove_manager_address = "0xA39739EF8b0231DbFA0DcdA07d7e29faAbCf4bb2"
price_feed_address = "0x4c517D4e2C851CA76d7eC94B805269Df0f2201De"

cache = {}


async def get_token_info(tokenInfo, provider):
    tm_contract = provider.eth.contract(
        address=trove_manager_address, abi=token_manager_abi)
    pf_contract = provider.eth.contract(
        address=price_feed_address, abi=price_feed_abi)

    trove_address = tokenInfo["trove"]
    if not (trove_address in cache):
        res = tm_contract.functions.getEntireDebtAndColl(trove_address).call()
        cache[trove_address] = {
            "borrow_token": res[0] / 10**18,
            "supply_token": res[1] / 10**18,
        }

    data = {}

    if (tokenInfo["symbol"] == "ETH"):
        data = {
            "supply_token": cache[trove_address]["supply_token"],
            "borrow_token": 0,
            "net_supply_token": cache[trove_address]["supply_token"],
            "non_recursive_supply_token": cache[trove_address]["supply_token"],
        }
    else:
        data = {
            "supply_token": 0,
            "borrow_token": cache[trove_address]["borrow_token"],
            "net_supply_token": 0,
            "non_recursive_supply_token": 0,
        }

    block = provider.eth.get_block_number()
    now = datetime.now()
    formatted_date = now.strftime("%Y-%m-%d")

    return {
        "protocol": "Nimbora",
        "date": formatted_date,
        "market": tokenInfo["address"],
        "tokenSymbol": tokenInfo["symbol"],
        "supply_token": data["supply_token"],
        "borrow_token": data["borrow_token"],
        "net_supply_token": data["net_supply_token"],
        "non_recursive_supply_token": data["non_recursive_supply_token"],
        "block_height": block,
        "lending_index_rate": "0"
    }


# Define functions


async def main():
    """
    Supply your calculation here according to the Guidelines.
    """

    node_url = os.environ["ETHEREUM_RPC"]
    provider = Web3(Web3.HTTPProvider(node_url))

    with open('./nimbora/borrow/tokens.json', 'r') as f:
        tokens = json.load(f)
        coroutines = [get_token_info(tokenInfo, provider)
                      for tokenInfo in tokens]
        gathered_results = await asyncio.gather(*coroutines)

        df = pd.DataFrame(gathered_results)
        df.to_csv('output_nimbora_borrow.csv', index=False)

        return gathered_results

if __name__ == "__main__":
    asyncio.run(main())
