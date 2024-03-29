import requests
import asyncio
from datetime import datetime
from starknet_py.contract import Contract
from starknet_py.net.full_node_client import FullNodeClient
import pandas as pd


NODE_URL = "https://starknet-mainnet.public.blastapi.io"
ASSETS = [{
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
    "asset_symbol": "DAI",
    "decimals": 18,
    "asset_address": "0x00da114221cb83fa859dbdb4c44beeaa0bb37c7537ad5ae66fe5e0efd20e6eb3",
    "i_token": "0x022ccca3a16c9ef0df7d56cbdccd8c4a6f98356dfd11abc61a112483b242db90",
    "i_token_c": "0x04f18ffc850cdfa223a530d7246d3c6fc12a5969e0aa5d4a88f470f5fe6c46e9",
    "d_token": "0x066037c083c33330a8460a65e4748ceec275bbf5f28aa71b686cbc0010e12597",
},
{
    "asset_symbol": "UNO",
    "decimals": 18,
    "asset_address": "0x0719b5092403233201aa822ce928bd4b551d0cdb071a724edd7dc5e5f57b7f34",
    "i_token": "0x01325caf7c91ee415b8df721fb952fa88486a0fc250063eafddd5d3c67867ce7",
    "i_token_c": "0x02a3a9d7bcecc6d3121e3b6180b73c7e8f4c5f81c35a90c8dd457a70a842b723",
    "d_token": "0x04b036839a8769c04144cc47415c64b083a2b26e4a7daa53c07f6042a0d35792",
},
]


client = FullNodeClient(node_url=NODE_URL)
async def main():
    eth_price = normalize(await get_pragma_eth_price(), 18)
    dai_price = normalize(await get_pragma_price("0x683852789848dea686fcfb66aaebf6477d83b25d8894aae73b15ff19b765bf0", "0x00da114221cb83fa859dbdb4c44beeaa0bb37c7537ad5ae66fe5e0efd20e6eb3"), 18) * eth_price
    usdc_price = normalize(await get_pragma_price("0x683852789848dea686fcfb66aaebf6477d83b25d8894aae73b15ff19b765bf0", "0x053c91253bc9682c04929ca02ed00b3e423f6710d2ee7e0d5ebb06f3ecf368a8"), 18) * eth_price
    usdt_price = normalize(await get_pragma_price("0x683852789848dea686fcfb66aaebf6477d83b25d8894aae73b15ff19b765bf0", "0x068f5c6a61780768455de69077e07e89787839bf8166decfbf92b645209c0fb8"), 18) * eth_price
    uno_price = normalize(await get_pragma_price("0x6661660c8201bf27c5799e819019e6e74914d5e9c6ed1458faeab403fc4b5c1", "0x0719b5092403233201aa822ce928bd4b551d0cdb071a724edd7dc5e5f57b7f34"), 18) * eth_price
    
    coroutines = [get_data(asset) for asset in ASSETS] + [get_stables_data(dai_price, usdc_price, usdt_price, uno_price)]
    individual_results = await asyncio.gather(*coroutines)

    # Generate the DataFrame
    df = pd.DataFrame(individual_results)

    # Prices mapping
    prices = {'DAI': dai_price, 'USDC': usdc_price, 'USDT': usdt_price, 'UNO': uno_price}

    # Calculate the sums directly, factoring in the prices
    supply_token_sum = sum(df.loc[df['tokenSymbol'] == symbol, 'supply_token'].iloc[0] * price for symbol, price in prices.items())
    borrow_token_sum = sum(df.loc[df['tokenSymbol'] == symbol, 'borrow_token'].iloc[0] * price for symbol, price in prices.items())
    net_supply_token_sum = supply_token_sum - borrow_token_sum

    # Update the 0x0stable/STB row
    df.loc[df['tokenSymbol'] == 'STB', 'supply_token'] = supply_token_sum
    df.loc[df['tokenSymbol'] == 'STB', 'borrow_token'] = borrow_token_sum
    df.loc[df['tokenSymbol'] == 'STB', 'net_supply_token'] = net_supply_token_sum

    # Write the updated DataFrame to a CSV
    df.to_csv('output_nostra.csv', index=False)



async def get_stables_data(dai_price, usdc_price, usdt_price, uno_price):
    block_height = await client.get_block_number()
    dai_index = normalize(await get_index("0x022ccca3a16c9ef0df7d56cbdccd8c4a6f98356dfd11abc61a112483b242db90", False, block_height), 18)
    usdc_index = normalize(await get_index("0x002fc2d4b41cc1f03d185e6681cbd40cced61915d4891517a042658d61cba3b1", False, block_height), 18)
    usdt_index = normalize(await get_index("0x0360f9786a6595137f84f2d6931aaec09ceec476a94a98dcad2bb092c6c06701", False, block_height), 18)
    uno_index = normalize(await get_index("0x01325caf7c91ee415b8df721fb952fa88486a0fc250063eafddd5d3c67867ce7", True, block_height), 18)

    stables_non_recursive_supply = normalize(await aggregate_stablecoins_non_recursive_supply(ASSETS, dai_index, usdc_index, usdt_index, uno_index, dai_price, usdc_price, usdt_price, uno_price), 18)


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
        "lending_index_rate": 1.0
    }

def normalize(value, decimals):
    return value / (10 ** decimals)

async def get_data(asset):
    is_cairo_v2_implementation = asset['asset_symbol'] == "STRK" or asset['asset_symbol'] == "UNO"
    protocol = "Nostra"
    formatted_date = datetime.now().strftime("%Y-%m-%d")
    market = asset["asset_address"]
    tokenSymbol = asset["asset_symbol"]
    block_height = await client.get_block_number()
    lending_index_rate_raw = await get_index(asset["i_token"], is_cairo_v2_implementation, block_height)
    non_recursive_supply_token_raw = get_non_recursive_supply(asset, lending_index_rate_raw)
    supply_token_raw = (await get_supply(asset["i_token"], is_cairo_v2_implementation, block_height)) + (await get_supply(asset["i_token_c"], is_cairo_v2_implementation, block_height))
    borrow_token_raw = await get_supply(asset["d_token"], is_cairo_v2_implementation, block_height)
    net_supply_token_raw = supply_token_raw - borrow_token_raw
    
    # Normalize the balance fields
    supply_token = normalize(supply_token_raw, asset['decimals'])
    borrow_token = normalize(borrow_token_raw, asset['decimals'])
    net_supply_token = normalize(net_supply_token_raw, asset['decimals'])
    non_recursive_supply_token = normalize(non_recursive_supply_token_raw, asset['decimals'])
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
        "lending_index_rate": lending_index_rate
    }



async def get_supply(address, is_cairo_v2_implementation, block_number):
    contract = await Contract.from_address(
        address=address,
        provider=client,
    )
    (value,) = await contract.functions["total_supply" if is_cairo_v2_implementation else "totalSupply"].call(block_number=block_number)
    return value

async def get_index(address, is_cairo_v2_implementation, block_number):
    contract = await Contract.from_address(
        address=address,
        provider=client,
    )
    (value,) = await contract.functions["token_index" if is_cairo_v2_implementation else "getTokenIndex"].call(block_number=block_number)
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
    return aggregate_non_recursive_supply_without_index(asset) * lending_index_rate / 1e18

def aggregate_non_recursive_supply_without_index(asset):
    QUERY_ENDPOINT = 'https://us-east-2.aws.data.mongodb-api.com/app/data-yqlpb/endpoint/data/v1/action/aggregate'
    DATA_SOURCE = 'nostra-production'
    DB = 'prod-a-nostra-db'
    COLLECTION = 'balances'
    
    pipeline = [
        # Match documents with the tokenAddress fields we're interested in (i_token, i_token_c, d_token)
        {
            "$match": {
                "tokenAddress": {
                    "$in": [asset["i_token"], asset["i_token_c"], asset["d_token"]]
                }
            }
        },
        # Project new fields to differentiate supply types and convert balanceWithoutIndex to decimal
        {
            "$addFields": {
                "supplyType": {
                    "$switch": {
                        "branches": [
                            {"case": {"$eq": ["$tokenAddress", asset["i_token"]]}, "then": "i_token"},
                            {"case": {"$eq": ["$tokenAddress", asset["i_token_c"]]}, "then": "i_token_c"}
                        ],
                        "default": "d_token"
                    }
                },
                "balanceDecimal": {"$toDecimal": "$balanceWithoutIndex"}
            }
        },
        # Group by accountAddress and calculate the sum of i_token, i_token_c, and d_token
        {
            "$group": {
                "_id": "$accountAddress",
                "i_token_sum": {"$sum": {"$cond": [{"$eq": ["$supplyType", "i_token"]}, "$balanceDecimal", 0]}},
                "i_token_c_sum": {"$sum": {"$cond": [{"$eq": ["$supplyType", "i_token_c"]}, "$balanceDecimal", 0]}},
                "d_token_sum": {"$sum": {"$cond": [{"$eq": ["$supplyType", "d_token"]}, "$balanceDecimal", 0]}}
            }
        },
        # Calculate the non-recursive supply per account, ensuring it doesn't go below 0
        {
            "$addFields": {
                "non_recursive_supply": {
                    "$max": [
                        0,
                        {"$subtract": [{"$add": ["$i_token_sum", "$i_token_c_sum"]}, "$d_token_sum"]}
                    ]
                }
            }
        },
        # Summarize total non-recursive supply across all accounts
        {
            "$group": {
                "_id": None,
                "total_non_recursive_supply": {"$sum": "$non_recursive_supply"}
            }
        }
    ]

    response = requests.post(QUERY_ENDPOINT, json={
        "dataSource": DATA_SOURCE,
        "database": DB,
        "collection": COLLECTION,
        "pipeline": pipeline
    })

    if response.status_code == 200:
        return int((response.json())["documents"][0]["total_non_recursive_supply"])
    else:
        print(f"Error: {response.status_code}")
        raise Exception(f"Error: {response.status_code}")


async def aggregate_stablecoins_non_recursive_supply(assets, dai_index, usdc_index, usdt_index, uno_index, dai_price, usdc_price, usdt_price, uno_price):
    QUERY_ENDPOINT = 'https://us-east-2.aws.data.mongodb-api.com/app/data-yqlpb/endpoint/data/v1/action/aggregate'
    DATA_SOURCE = 'nostra-production'
    DB = 'prod-a-nostra-db'
    COLLECTION = 'balances'
    
    stablecoin_addresses = {
        "DAI": {
            "i_token": "0x022ccca3a16c9ef0df7d56cbdccd8c4a6f98356dfd11abc61a112483b242db90",
            "i_token_c": "0x04f18ffc850cdfa223a530d7246d3c6fc12a5969e0aa5d4a88f470f5fe6c46e9",
            "d_token": "0x066037c083c33330a8460a65e4748ceec275bbf5f28aa71b686cbc0010e12597",
        },
        "USDC": {
            "i_token": "0x002fc2d4b41cc1f03d185e6681cbd40cced61915d4891517a042658d61cba3b1",
            "i_token_c": "0x05dcd26c25d9d8fd9fc860038dcb6e4d835e524eb8a85213a8cda5b7fff845f6",
            "d_token": "0x063d69ae657bd2f40337c39bf35a870ac27ddf91e6623c2f52529db4c1619a51"
        },
        "USDT": {
            "i_token": "0x0360f9786a6595137f84f2d6931aaec09ceec476a94a98dcad2bb092c6c06701",
            "i_token_c": "0x0453c4c996f1047d9370f824d68145bd5e7ce12d00437140ad02181e1d11dc83",
            "d_token": "0x024e9b0d6bc79e111e6872bb1ada2a874c25712cf08dfc5bcf0de008a7cca55f"
        },
        "UNO": {
            "i_token": "0x01325caf7c91ee415b8df721fb952fa88486a0fc250063eafddd5d3c67867ce7",
            "i_token_c": "0x02a3a9d7bcecc6d3121e3b6180b73c7e8f4c5f81c35a90c8dd457a70a842b723",
            "d_token": "0x04b036839a8769c04144cc47415c64b083a2b26e4a7daa53c07f6042a0d35792"
        }
    }

    # Flatten the addresses to a single list for the $in query operator
    token_addresses = [address for asset in stablecoin_addresses.values() for address in asset.values()]
    
    pipeline = [
        {
            "$match": {
                "tokenAddress": {
                    "$in": token_addresses
                }
            }
        },
        {
            "$addFields": {
                "dai_index": {"$toDecimal": dai_index},
                "usdc_index": {"$toDecimal": usdc_index},
                "usdt_index": {"$toDecimal": usdt_index},
                "uno_index": {"$toDecimal": uno_index},
                "dai_price": {"$toDecimal": dai_price},
                "usdc_price": {"$toDecimal": usdc_price},
                "usdt_price": {"$toDecimal": usdt_price},
                "uno_price": {"$toDecimal": uno_price},
            }
        },
        {
            "$addFields": {
                "supplyType": {
                    "$switch": {
                        "branches": [
                            {"case": {"$in": ["$tokenAddress", [stablecoin_addresses["DAI"]["i_token"], stablecoin_addresses["USDC"]["i_token"], stablecoin_addresses["USDT"]["i_token"], stablecoin_addresses["UNO"]["i_token"]]]}, "then": "i_token"},
                            {"case": {"$in": ["$tokenAddress", [stablecoin_addresses["DAI"]["i_token_c"], stablecoin_addresses["USDC"]["i_token_c"], stablecoin_addresses["USDT"]["i_token_c"], stablecoin_addresses["UNO"]["i_token_c"]]]}, "then": "i_token_c"}
                        ],
                        "default": "d_token"
                    }
                },
                "normalizedBalance": {
                    "$cond": {
                        "if": {"$eq": ["$asset", "USDT"]},
                        "then": {"$multiply": [{"$toDecimal": "$balanceWithoutIndex"}, "$usdt_index", "$usdt_price", 1e12]},
                        "else": {
                            "$cond": {
                                "if": {"$eq": ["$asset", "USDC"]},
                                "then": {"$multiply": [{"$toDecimal": "$balanceWithoutIndex"}, "$usdc_index", "$usdc_price", 1e12]},
                                "else": {
                                    "$cond": {
                                        "if": {"$eq": ["$asset", "DAI"]},
                                        "then": {"$multiply": [{"$toDecimal": "$balanceWithoutIndex"}, "$dai_index", "$dai_price"]},
                                        "else": {"$multiply": [{"$toDecimal": "$balanceWithoutIndex"}, "$uno_index", "$uno_price"]}
                                    }
                                }
                            }
                        }
                    }
                }
            }
        },
        {
            "$group": {
                "_id": "$accountAddress",
                "i_token_sum": {"$sum": {"$cond": [{"$eq": ["$supplyType", "i_token"]}, "$normalizedBalance", 0]}},
                "i_token_c_sum": {"$sum": {"$cond": [{"$eq": ["$supplyType", "i_token_c"]}, "$normalizedBalance", 0]}},
                "d_token_sum": {"$sum": {"$cond": [{"$eq": ["$supplyType", "d_token"]}, "$normalizedBalance", 0]}}
            }
        },
        {
            "$addFields": {
                "non_recursive_supply": {
                    "$max": [
                        0,
                        {"$subtract": [{"$add": ["$i_token_sum", "$i_token_c_sum"]}, "$d_token_sum"]}
                    ]
                }
            }
        },
        {
            "$group": {
                "_id": None,
                "total_non_recursive_supply": {"$sum": "$non_recursive_supply"}
            }
        }
    ]

    response = requests.post(QUERY_ENDPOINT, json={
        "dataSource": DATA_SOURCE,
        "database": DB,
        "collection": COLLECTION,
        "pipeline": pipeline
    })

    if response.status_code == 200:
        data = response.json()["documents"][0]
        return round(float(data["total_non_recursive_supply"]))
    else:
        print(f"Error: {response.status_code}")
        raise Exception(f"Error: {response.status_code}")



if __name__ == "__main__":
    asyncio.run(main())