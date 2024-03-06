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
]


client = FullNodeClient(node_url=NODE_URL)

async def main():
    coroutines = [get_data(asset) for asset in ASSETS]
    gathered_results = await asyncio.gather(*coroutines)
    
    df = pd.DataFrame(gathered_results)
    df.to_csv('output_nostra.csv', index=False)


def normalize(value, decimals):
    return value / (10 ** decimals)

async def get_data(asset):
    is_cairo_v2_implementation = asset['asset_symbol'] == "STRK"
    protocol = "Nostra"
    formatted_date = datetime.now().strftime("%Y-%m-%d")
    market = asset["asset_address"]
    tokenSymbol = asset["asset_symbol"]
    block_height = await client.get_block_number()
    supply_token_raw = (await get_supply(asset["i_token"], is_cairo_v2_implementation)) + (await get_supply(asset["i_token_c"], is_cairo_v2_implementation))
    borrow_token_raw = await get_supply(asset["d_token"], is_cairo_v2_implementation)
    net_supply_token_raw = supply_token_raw - borrow_token_raw
    lending_index_rate = await get_index(asset["i_token"], is_cairo_v2_implementation)
    
    # Normalize the balance fields
    supply_token = normalize(supply_token_raw, asset['decimals'])
    borrow_token = normalize(borrow_token_raw, asset['decimals'])
    net_supply_token = normalize(net_supply_token_raw, asset['decimals'])
    non_recursive_supply_token_raw = get_non_recursive_supply(asset, lending_index_rate)
    non_recursive_supply_token = normalize(non_recursive_supply_token_raw, asset['decimals'])

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
        "lending_index_rate": normalize(lending_index_rate, 18)
    }



async def get_supply(address, is_cairo_v2_implementation):
    contract = await Contract.from_address(
        address=address,
        provider=client,
    )
    (value,) = await contract.functions["total_supply" if is_cairo_v2_implementation else "totalSupply"].call()
    return value

async def get_index(address, is_cairo_v2_implementation=False):
    contract = await Contract.from_address(
        address=address,
        provider=client,
    )
    (value,) = await contract.functions["token_index" if is_cairo_v2_implementation else "getTokenIndex"].call()
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


if __name__ == "__main__":
    asyncio.run(main())