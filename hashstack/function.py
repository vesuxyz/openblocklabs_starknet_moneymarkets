import asyncio, json
from pandas import DataFrame
from starknet_py.contract import Contract
from starknet_py.net.full_node_client import FullNodeClient
from datetime import datetime
import pandas as pd
from collections import OrderedDict

rTokenAbi = json.load(open('./hashstack/rToken.abi.json'))
dTokenAbi = json.load(open('./hashstack/dToken.abi.json'))

# Returns row for each token
# Contains supply and lending index
async def get_token_info(tokenInfo, provider):
    supply_contract = Contract(
        address=tokenInfo['rToken'], abi=rTokenAbi, provider=provider
    )

    (total_assets,) = await supply_contract.functions["total_assets"].call()

    (_, lending_rate) = await supply_contract.functions["exchange_rate"].call()

    borrow_contract = Contract(
        address=tokenInfo['dToken'], abi=dTokenAbi, provider=provider
    )

    (total_debt,) = await borrow_contract.functions["totalDebt"].call()

    net_supply = total_assets - total_debt
    block = await provider.get_block_number()
    now = datetime.now()
    formatted_date = now.strftime("%Y-%m-%d")

    return {
        "protocol": "Hashstack",
        "date": formatted_date,
        "market": tokenInfo["address"],
        "tokenSymbol": tokenInfo["name"],
        "supply_token": total_assets / 10 ** tokenInfo['decimals'],
        "borrow_token": total_debt / 10 ** tokenInfo['decimals'],
        "net_supply_token": net_supply / 10 ** tokenInfo['decimals'],
        "non_recursive_supply_token": total_assets / 10 ** tokenInfo['decimals'],
        "block_height": block,
        "lending_index_rate": lending_rate/10**18
    }

def combine_stables(data):
    stables = ['USDC', 'USDT', 'DAI']
    return {
        "protocol": "Hashstack",
        "date": data[0]['date'],
        "market": "0x0stable",
        "tokenSymbol": "STB",
        "supply_token": sum([row['supply_token'] for row in data if row['tokenSymbol'] in stables ]),
        "borrow_token": sum([row['borrow_token'] for row in data if row['tokenSymbol'] in stables ]),
        "net_supply_token": sum([row['net_supply_token'] for row in data if row['tokenSymbol'] in stables ]),
        "non_recursive_supply_token": sum([row['non_recursive_supply_token'] for row in data if row['tokenSymbol'] in stables ]),
        "block_height": data[0]['block_height'],
        "lending_index_rate": 1
    }

# Define functions
async def main():
    """
    Supply your calculation here according to the Guidelines.
    """

    node_url = "https://starknet-mainnet.public.blastapi.io"
    provider = FullNodeClient(node_url=node_url)

    with open('./hashstack/tokens.json', 'r') as f:
        tokens = json.load(f)
        coroutines = [get_token_info(tokenInfo, provider) for tokenInfo in tokens]
        gathered_results = await asyncio.gather(*coroutines)
        
        stables_combined_row = combine_stables(gathered_results)
        gathered_results.append(stables_combined_row)

        df = pd.DataFrame(gathered_results)
        df.to_csv('output_hashstack.csv', index=False)

        return gathered_results

if __name__ == "__main__":
    asyncio.run(main())