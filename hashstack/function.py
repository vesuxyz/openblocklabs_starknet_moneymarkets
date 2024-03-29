import asyncio, json
from pandas import DataFrame
from starknet_py.contract import Contract
from starknet_py.net.full_node_client import FullNodeClient
from datetime import datetime
import pandas as pd
from collections import OrderedDict

rTokenAbi = json.load(open('./hashstack/rToken.abi.json'))
dTokenAbi = json.load(open('./hashstack/dToken.abi.json'))

ORACLE_ADDRESS = '0x683852789848dea686fcfb66aaebf6477d83b25d8894aae73b15ff19b765bf0'
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

def normalize(value, decimals):
    return value / (10 ** decimals)

async def get_pragma_eth_price(client):
    # nostra oracle (uses pragma under the hood)
    contract = await Contract.from_address(
        address=ORACLE_ADDRESS,
        provider=client,
    )
    (value,) = await contract.functions["getBaseAssetPriceInUsd"].call()
    return value

async def get_pragma_price(asset, client):
    # nostra oracle (uses pragma under the hood)
    contract = await Contract.from_address(
        address=ORACLE_ADDRESS,
        provider=client,
    )
    (value,) = await contract.functions["getAssetPrice"].call(int(asset, 16))
    return value

async def combine_stables(data, client):
    stables = ['USDC', 'USDT', 'DAI']
    eth_price = normalize(await get_pragma_eth_price(client), 18)
    usdt_price = normalize(await get_pragma_price("0x068f5c6a61780768455de69077e07e89787839bf8166decfbf92b645209c0fb8", client), 18) * eth_price
    usdc_price = normalize(await get_pragma_price("0x053c91253bc9682c04929ca02ed00b3e423f6710d2ee7e0d5ebb06f3ecf368a8", client), 18) * eth_price
    dai_price = normalize(await get_pragma_price("0x00da114221cb83fa859dbdb4c44beeaa0bb37c7537ad5ae66fe5e0efd20e6eb3", client), 18) * eth_price
    
    print(eth_price, usdc_price, usdt_price, dai_price)
    prices = {'DAI': dai_price, 'USDC': usdc_price, 'USDT': usdt_price}
    return {
        "protocol": "Hashstack",
        "date": data[0]['date'],
        "market": "0x0stable",
        "tokenSymbol": "STB",
        "supply_token": sum([row['supply_token'] * prices[row['tokenSymbol']] for row in data if row['tokenSymbol'] in stables ]),
        "borrow_token": sum([row['borrow_token'] * prices[row['tokenSymbol']] for row in data if row['tokenSymbol'] in stables ]),
        "net_supply_token": sum([row['net_supply_token'] * prices[row['tokenSymbol']] for row in data if row['tokenSymbol'] in stables ]),
        "non_recursive_supply_token": sum([row['non_recursive_supply_token'] * prices[row['tokenSymbol']] for row in data if row['tokenSymbol'] in stables ]),
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
        
        stables_combined_row = await combine_stables(gathered_results, provider)
        gathered_results.append(stables_combined_row)

        df = pd.DataFrame(gathered_results)
        df.to_csv('output_hashstack.csv', index=False)

        return gathered_results

if __name__ == "__main__":
    asyncio.run(main())