import asyncio
import json
from starknet_py.contract import Contract
from starknet_py.net.full_node_client import FullNodeClient
from datetime import datetime
import pandas as pd
from decimal import Decimal

mTokenAbi = json.load(open('./openblocklabs_starknet_moneymarkets/protocols/nimbora/lend/mToken.abi.json'))
PRAGMA_ADAPTER_ABI = json.load(open("./openblocklabs_starknet_moneymarkets/protocols/zklend/pragma_adapter.abi.json"))


async def get_pragma_dai_price(block_height, client):
    contract = Contract(
        address=int('0x05d1bc06ca368cc451f63b20bc12bd2299a4ae7776f4dcf977723839bef311a0', 16),
        abi=PRAGMA_ADAPTER_ABI,
        provider=client,
    )
    (uint_raw_price,) = await contract.functions["get_price"].call(
        block_number=block_height
    )
    decimal_price = Decimal(uint_raw_price) / Decimal(f"1e8")
    return decimal_price


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



async def combine_stables(data, client):
    stable = "DAI"
    block = data[0]["block_height"]
    dai_price = await get_pragma_dai_price(block, client)    
    supply_token = dai_price * Decimal(data[0]["supply_token"])

    return {
        "protocol": "Nimbora",
        "date": data[0]["date"],
        "market": "0x0stable",
        "tokenSymbol": "STB",
        "supply_token": supply_token,
        "borrow_token": 0,
        "net_supply_token": supply_token,
        "non_recursive_supply_token": supply_token,
        "block_height": block,
        "lending_index_rate": 1,
    }


async def main():
    """
    Supply your calculation here according to the Guidelines.
    """

    node_url = "https://starknet-mainnet.public.blastapi.io"
    provider = FullNodeClient(node_url=node_url)

    with open('./openblocklabs_starknet_moneymarkets/protocols/nimbora/lend/tokens.json', 'r') as f:
        tokens = json.load(f)
        coroutines = [get_token_info(tokenInfo, provider)
                      for tokenInfo in tokens]
        gathered_results = await asyncio.gather(*coroutines)

        stables_combined_row = await combine_stables(gathered_results, provider)
        gathered_results.append(stables_combined_row)

        df = pd.DataFrame(gathered_results)

        return df

if __name__ == "__main__":
    asyncio.run(main())
