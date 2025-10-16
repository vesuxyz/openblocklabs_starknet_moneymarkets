import requests
import asyncio
from datetime import datetime
from starknet_py.contract import Contract
from starknet_py.net.full_node_client import FullNodeClient
import pandas as pd

NODE_URL = "https://starknet-mainnet.public.blastapi.io"
ASSETS = [
    {
        "asset_symbol": "STRK",
        "decimals": 18,
        "asset_address": "0x04718f5a0fc34cc1af16a1cdee98ffb20c31f5cd61d6ab07201858f4287c938d",
        "i_token": "0x026c5994c2462770bbf940552c5824fb0e0920e2a8a5ce1180042da1b3e489db",
        "i_token_c": "0x07c2e1e733f28daa23e78be3a4f6c724c0ab06af65f6a95b5e0545215f1abc1b",
        "d_token": "0x001258eae3eae5002125bebf062d611a772e8aea3a1879b64a19f363ebd00947",
    },
    {
        "asset_symbol": "ETH",
        "decimals": 18,
        "asset_address": "0x049d36570d4e46f48e99674bd3fcc84644ddd6b96f7c741b1562b82f9e004dc7",
        "i_token": "0x01fecadfe7cda2487c66291f2970a629be8eecdcb006ba4e71d1428c2b7605c7",
        "i_token_c": "0x057146f6409deb4c9fa12866915dd952aa07c1eb2752e451d7f3b042086bdeb8",
        "d_token": "0x00ba3037d968790ac486f70acaa9a1cab10cf5843bb85c986624b4d0e5a82e74",
    },
    {
        "asset_symbol": "USDC",
        "decimals": 6,
        "asset_address": "0x053c91253bc9682c04929ca02ed00b3e423f6710d2ee7e0d5ebb06f3ecf368a8",
        "i_token": "0x002fc2d4b41cc1f03d185e6681cbd40cced61915d4891517a042658d61cba3b1",
        "i_token_c": "0x05dcd26c25d9d8fd9fc860038dcb6e4d835e524eb8a85213a8cda5b7fff845f6",
        "d_token": "0x063d69ae657bd2f40337c39bf35a870ac27ddf91e6623c2f52529db4c1619a51",
    },
    {
        "asset_symbol": "USDT",
        "decimals": 6,
        "asset_address": "0x068f5c6a61780768455de69077e07e89787839bf8166decfbf92b645209c0fb8",
        "i_token": "0x0360f9786a6595137f84f2d6931aaec09ceec476a94a98dcad2bb092c6c06701",
        "i_token_c": "0x0453c4c996f1047d9370f824d68145bd5e7ce12d00437140ad02181e1d11dc83",
        "d_token": "0x024e9b0d6bc79e111e6872bb1ada2a874c25712cf08dfc5bcf0de008a7cca55f",
    },
    {
        "asset_symbol": "DAI_V0",
        "decimals": 18,
        "asset_address": "0x00da114221cb83fa859dbdb4c44beeaa0bb37c7537ad5ae66fe5e0efd20e6eb3",
        "i_token": "0x022ccca3a16c9ef0df7d56cbdccd8c4a6f98356dfd11abc61a112483b242db90",
        "i_token_c": "0x04f18ffc850cdfa223a530d7246d3c6fc12a5969e0aa5d4a88f470f5fe6c46e9",
        "d_token": "0x066037c083c33330a8460a65e4748ceec275bbf5f28aa71b686cbc0010e12597",
    },
    {
        "asset_symbol": "DAI",
        "decimals": 18,
        "asset_address": "0x05574eb6b8789a91466f902c380d978e472db68170ff82a5b650b95a58ddf4ad",
        "i_token": "0x065bde349f553cf4bdd873e54cd48317eda0542764ebe5ba46984cedd940a5e4",
        "i_token_c": None,
        "d_token": "0x06726ec97bae4e28efa8993a8e0853bd4bad0bd71de44c23a1cd651b026b00e7",
    },
    {
        "asset_symbol": "UNO",
        "decimals": 18,
        "asset_address": "0x0719b5092403233201aa822ce928bd4b551d0cdb071a724edd7dc5e5f57b7f34",
        "i_token": "0x01325caf7c91ee415b8df721fb952fa88486a0fc250063eafddd5d3c67867ce7",
        "i_token_c": "0x02a3a9d7bcecc6d3121e3b6180b73c7e8f4c5f81c35a90c8dd457a70a842b723",
        "d_token": "0x04b036839a8769c04144cc47415c64b083a2b26e4a7daa53c07f6042a0d35792",
    },
    {
        "asset_symbol": "wstETH",
        "decimals": 18,
        "asset_address": "0x042b8f0484674ca266ac5d08e4ac6a3fe65bd3129795def2dca5c34ecc5f96d2",
        "i_token": "0x00ca44c79a77bcb186f8cdd1a0cd222cc258bebc3bec29a0a020ba20fdca40e9",
        "i_token_c": "0x009377fdde350e01e0397820ea83ed3b4f05df30bfb8cf8055d62cafa1b2106a",
        "d_token": "0x0348cc417fc877a7868a66510e8e0d0f3f351f5e6b0886a86b652fcb30a3d1fb",
    },
    {
        "asset_symbol": "xSTRK",
        "decimals": 18,
        "asset_address": "0x028d709c875c0ceac3dce7065bec5328186dc89fe254527084d1689910954b0a",
        "i_token": "0x04d1125a716f547a0b69413c0098e811da3b799d173429c95da4290a00c139f7",
        "i_token_c": "0x0257afe480da9255a026127cd3a295a580ef316b297a69be22b89729ae8c1d2a",
        "d_token": "0x0424638c9060d08b4820aabbb28347fc7234e2b7aadab58ad0f101e2412ea42d",
    },
    {
        "asset_symbol": "WBTC",
        "decimals": 8,
        "asset_address": "0x03fe2b97c1fd336e750087d68b9b867997fd64a2661ff3ca5a7c771641e8e7ac",
        "i_token": "0x0735d0f09a4e8bf8a17005fa35061b5957dcaa56889fc75df9e94530ff6991ea",
        "i_token_c": "0x05b7d301fa769274f20e89222169c0fad4d846c366440afc160aafadd6f88f0c",
        "d_token": "0x0491480f21299223b9ce770f23a2c383437f9fbf57abc2ac952e9af8cdb12c97",
    },
]


client = FullNodeClient(node_url=NODE_URL)


async def main():
    eth_price = normalize(await get_pragma_eth_price(), 18)
    dai_price = (
        normalize(
            await get_pragma_price(
                "0x683852789848dea686fcfb66aaebf6477d83b25d8894aae73b15ff19b765bf0",
                "0x00da114221cb83fa859dbdb4c44beeaa0bb37c7537ad5ae66fe5e0efd20e6eb3",
            ),
            18,
        )
        * eth_price
    )
    usdc_price = (
        normalize(
            await get_pragma_price(
                "0x683852789848dea686fcfb66aaebf6477d83b25d8894aae73b15ff19b765bf0",
                "0x053c91253bc9682c04929ca02ed00b3e423f6710d2ee7e0d5ebb06f3ecf368a8",
            ),
            18,
        )
        * eth_price
    )
    usdt_price = (
        normalize(
            await get_pragma_price(
                "0x683852789848dea686fcfb66aaebf6477d83b25d8894aae73b15ff19b765bf0",
                "0x068f5c6a61780768455de69077e07e89787839bf8166decfbf92b645209c0fb8",
            ),
            18,
        )
        * eth_price
    )
    uno_price = (
        normalize(
            await get_pragma_price(
                "0x6661660c8201bf27c5799e819019e6e74914d5e9c6ed1458faeab403fc4b5c1",
                "0x0719b5092403233201aa822ce928bd4b551d0cdb071a724edd7dc5e5f57b7f34",
            ),
            18,
        )
        * eth_price
    )

    coroutines = [get_data(asset) for asset in ASSETS] + [
        get_stables_data(dai_price, usdc_price, usdt_price, uno_price)
    ]
    individual_results = await asyncio.gather(*coroutines)

    # Generate the DataFrame
    df = pd.DataFrame(individual_results)

    # Prices mapping
    prices = {
        "DAI_V0": dai_price,
        "DAI": dai_price,
        "USDC": usdc_price,
        "USDT": usdt_price,
        "UNO": uno_price,
    }

    # Calculate the sums directly, factoring in the prices
    supply_token_sum = sum(
        df.loc[df["tokenSymbol"] == symbol, "supply_token"].iloc[0] * price
        for symbol, price in prices.items()
    )
    borrow_token_sum = sum(
        df.loc[df["tokenSymbol"] == symbol, "borrow_token"].iloc[0] * price
        for symbol, price in prices.items()
    )
    net_supply_token_sum = supply_token_sum - borrow_token_sum

    # Update the 0x0stable/STB row
    df.loc[df["tokenSymbol"] == "STB", "supply_token"] = supply_token_sum
    df.loc[df["tokenSymbol"] == "STB", "borrow_token"] = borrow_token_sum
    df.loc[df["tokenSymbol"] == "STB", "net_supply_token"] = net_supply_token_sum

    return df
    # Write the updated DataFrame to a CSV
    # df.to_csv('output_nostra.csv', index=False)


async def get_stables_data(dai_price, usdc_price, usdt_price, uno_price):
    block_height = await client.get_block_number()
    stables_non_recursive_supply = normalize(
        await aggregate_stablecoins_non_recursive_supply(),
        18,
    )

    return {
        "protocol": "Nostra",
        "date": datetime.now().strftime("%Y-%m-%d"),
        "market": "0x0stable",
        "tokenSymbol": "STB",
        "block_height": block_height,
        "supply_token": 0,
        "borrow_token": 0,
        "net_supply_token": 0,
        "non_recursive_supply_token": stables_non_recursive_supply,
        "lending_index_rate": 1.0,
    }


def normalize(value, decimals):
    return value / (10**decimals)


async def get_data(asset):
    protocol = "Nostra"
    formatted_date = datetime.now().strftime("%Y-%m-%d")
    market = asset["asset_address"]
    tokenSymbol = asset["asset_symbol"]
    block_height = await client.get_block_number()
    lending_index_rate_raw = await get_index(
        asset["i_token"], block_height
    )
    non_recursive_supply_token_raw = get_non_recursive_supply(
        asset, lending_index_rate_raw
    )
    supply_token_raw = (await get_supply(asset["i_token"], block_height)) + (
        await get_supply(asset["i_token_c"], block_height)
        if asset["i_token_c"] is not None
        else 0
    )

    borrow_token_raw = await get_supply(asset["d_token"], block_height)
    net_supply_token_raw = supply_token_raw - borrow_token_raw

    # Normalize the balance fields
    supply_token = normalize(supply_token_raw, asset["decimals"])
    borrow_token = normalize(borrow_token_raw, asset["decimals"])
    net_supply_token = normalize(net_supply_token_raw, asset["decimals"])
    non_recursive_supply_token = normalize(
        non_recursive_supply_token_raw, asset["decimals"]
    )
    lending_index_rate = normalize(lending_index_rate_raw, 18)

    return {
        "protocol": protocol,
        "date": formatted_date,
        "market": market,
        "tokenSymbol": tokenSymbol,
        "block_height": block_height,
        "supply_token": supply_token,
        "borrow_token": borrow_token,
        "net_supply_token": net_supply_token,
        "non_recursive_supply_token": non_recursive_supply_token,
        "lending_index_rate": lending_index_rate,
    }


async def get_supply(address, block_number):
    contract = await Contract.from_address(
        address=address,
        provider=client,
    )
    (value,) = await contract.functions["total_supply"].call(block_number=block_number)
    return value


async def get_index(address, block_number):
    contract = await Contract.from_address(
        address=address,
        provider=client,
    )
    try:
        (value,) = await contract.functions["token_index"].call(block_number=block_number)
    except Exception:
        (value,) = await contract.functions["getTokenIndex"].call(block_number=block_number)
    return value

async def get_pragma_eth_price():
    # nostra oracle (uses pragma under the hood)
    contract = await Contract.from_address(
        address="0x683852789848dea686fcfb66aaebf6477d83b25d8894aae73b15ff19b765bf0",
        provider=client,
    )
    (value,) = await contract.functions["getBaseAssetPriceInUsd"].call()
    return value


async def get_pragma_price(oracle_address, asset):
    # nostra oracle (uses pragma under the hood)
    contract = await Contract.from_address(
        address=oracle_address,
        provider=client,
    )
    (value,) = await contract.functions["getAssetPrice"].call(int(asset, 16))
    return value


def get_non_recursive_supply(asset, lending_index_rate):
    return (
        aggregate_non_recursive_supply_without_index(asset) * lending_index_rate / 1e18
    )


def aggregate_non_recursive_supply_without_index(asset):
    response = requests.get(
        "https://api.nostra.finance/openblock/nrs/" + asset["asset_symbol"],
    )

    if response.status_code == 200:
        return int(response.json()["documents"][0]["total_non_recursive_supply"]["$numberDecimal"])
    else:
        print(f"Error: {response.status_code}")
        raise Exception(f"Error: {response.status_code}")


async def aggregate_stablecoins_non_recursive_supply():
    response = requests.get("https://api.nostra.finance/openblock/nrs/STB")

    if response.status_code == 200:
        data = response.json()["documents"][0]
        return round(float(data["total_non_recursive_supply"]["$numberDecimal"]))
    else:
        print(f"Error: {response.status_code}")
        raise Exception(f"Error: {response.status_code}")