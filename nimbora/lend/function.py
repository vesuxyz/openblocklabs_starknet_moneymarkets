import asyncio
import json
from pandas import DataFrame
from starknet_py.contract import Contract
from starknet_py.net.full_node_client import FullNodeClient
from datetime import datetime
import pandas as pd
from collections import OrderedDict

mTokenAbi = json.load(open('./nimbora/lend/mToken.abi.json'))

# Returns row for each token
# Contains supply and lending index


async def get_token_info(tokenInfo, provider):
    token_manager_contract = Contract(
        address=tokenInfo['mToken'], abi=mTokenAbi, provider=provider
    )

    total_assets = (await token_manager_contract.functions["total_assets"].call())[0] / 10**18
    lending_rate = (await token_manager_contract.functions["convert_to_assets"].call({"low": 1000000000000000000, "high": 0}))[0] / 10**18

    block = await provider.get_block_number()
    now = datetime.now()
    formatted_date = now.strftime("%Y-%m-%d")

    return {
        "protocol": "Nimbora",
        "date": formatted_date,
        "market": tokenInfo["address"],
        "tokenSymbol": tokenInfo["name"],
        "supply_token": total_assets,
        "borrow_token": 0,
        "net_supply_token": total_assets,
        "non_recursive_supply_token": total_assets,
        "block_height": block,
        "lending_index_rate": lending_rate
    }

# Define functions


async def main():
    """
    Supply your calculation here according to the Guidelines.
    """

    node_url = "https://starknet-mainnet.public.blastapi.io"
    provider = FullNodeClient(node_url=node_url)

    with open('./nimbora/lend/tokens.json', 'r') as f:
        tokens = json.load(f)
        coroutines = [get_token_info(tokenInfo, provider)
                      for tokenInfo in tokens]
        gathered_results = await asyncio.gather(*coroutines)

        df = pd.DataFrame(gathered_results)
        df.to_csv('output_nimbora_lend.csv', index=False)

        return gathered_results

if __name__ == "__main__":
    asyncio.run(main())
