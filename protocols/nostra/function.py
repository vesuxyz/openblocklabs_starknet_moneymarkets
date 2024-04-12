import os
os.environ["AWS_DEFAULT_REGION"] = "us-east-1"

import sys
sys.path.append(".")

import requests
import asyncio
from datetime import datetime, timedelta, timezone
from starknet_py.contract import Contract
from starknet_py.net.full_node_client import FullNodeClient
import pandas as pd

from utils.s3_utils import load_parquet_to_s3, read_parquet_from_s3
from utils.athena_utils import get_athena_prices_hourly, get_athena_uno_prices_hourly
from utils.logging_utils import print_and_log
from utils.snowflake_utils import get_snowflake_strk_prices_hourly


# Set this False if just testing
write_to_s3 = True

PROTOCOL = "nostra"
BUCKET = "starknet-openblocklabs"
S3_FILEPATH = f"grant_scores_lending_test/grant_scores_lending_{PROTOCOL}.parquet"

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
    dai_index = normalize(
        await get_index(
            "0x022ccca3a16c9ef0df7d56cbdccd8c4a6f98356dfd11abc61a112483b242db90",
            False,
            block_height,
        ),
        18,
    )
    usdc_index = normalize(
        await get_index(
            "0x002fc2d4b41cc1f03d185e6681cbd40cced61915d4891517a042658d61cba3b1",
            False,
            block_height,
        ),
        18,
    )
    usdt_index = normalize(
        await get_index(
            "0x0360f9786a6595137f84f2d6931aaec09ceec476a94a98dcad2bb092c6c06701",
            False,
            block_height,
        ),
        18,
    )
    uno_index = normalize(
        await get_index(
            "0x01325caf7c91ee415b8df721fb952fa88486a0fc250063eafddd5d3c67867ce7",
            True,
            block_height,
        ),
        18,
    )

    stables_non_recursive_supply = normalize(
        await aggregate_stablecoins_non_recursive_supply(
            ASSETS,
            dai_index,
            usdc_index,
            usdt_index,
            uno_index,
            dai_price,
            usdc_price,
            usdt_price,
            uno_price,
        ),
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
    is_cairo_v2_implementation = (
        asset["asset_symbol"] == "STRK" or asset["asset_symbol"] == "UNO"
    )
    protocol = "Nostra"
    formatted_date = datetime.now().strftime("%Y-%m-%d")
    market = asset["asset_address"]
    tokenSymbol = asset["asset_symbol"]
    block_height = await client.get_block_number()
    lending_index_rate_raw = await get_index(
        asset["i_token"], is_cairo_v2_implementation, block_height
    )
    non_recursive_supply_token_raw = get_non_recursive_supply(
        asset, lending_index_rate_raw
    )
    supply_token_raw = (
        await get_supply(asset["i_token"], is_cairo_v2_implementation, block_height)
    ) + (await get_supply(asset["i_token_c"], is_cairo_v2_implementation, block_height))
    borrow_token_raw = await get_supply(
        asset["d_token"], is_cairo_v2_implementation, block_height
    )
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


async def get_supply(address, is_cairo_v2_implementation, block_number):
    contract = await Contract.from_address(
        address=address,
        provider=client,
    )
    (value,) = await contract.functions[
        "total_supply" if is_cairo_v2_implementation else "totalSupply"
    ].call(block_number=block_number)
    return value


async def get_index(address, is_cairo_v2_implementation, block_number):
    contract = await Contract.from_address(
        address=address,
        provider=client,
    )
    (value,) = await contract.functions[
        "token_index" if is_cairo_v2_implementation else "getTokenIndex"
    ].call(block_number=block_number)
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
    QUERY_ENDPOINT = "https://us-east-2.aws.data.mongodb-api.com/app/data-yqlpb/endpoint/data/v1/action/aggregate"
    DATA_SOURCE = "nostra-production"
    DB = "prod-a-nostra-db"
    COLLECTION = "balances"

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
                            {
                                "case": {"$eq": ["$tokenAddress", asset["i_token"]]},
                                "then": "i_token",
                            },
                            {
                                "case": {"$eq": ["$tokenAddress", asset["i_token_c"]]},
                                "then": "i_token_c",
                            },
                        ],
                        "default": "d_token",
                    }
                },
                "balanceDecimal": {"$toDecimal": "$balanceWithoutIndex"},
            }
        },
        # Group by accountAddress and calculate the sum of i_token, i_token_c, and d_token
        {
            "$group": {
                "_id": "$accountAddress",
                "i_token_sum": {
                    "$sum": {
                        "$cond": [
                            {"$eq": ["$supplyType", "i_token"]},
                            "$balanceDecimal",
                            0,
                        ]
                    }
                },
                "i_token_c_sum": {
                    "$sum": {
                        "$cond": [
                            {"$eq": ["$supplyType", "i_token_c"]},
                            "$balanceDecimal",
                            0,
                        ]
                    }
                },
                "d_token_sum": {
                    "$sum": {
                        "$cond": [
                            {"$eq": ["$supplyType", "d_token"]},
                            "$balanceDecimal",
                            0,
                        ]
                    }
                },
            }
        },
        # Calculate the non-recursive supply per account, ensuring it doesn't go below 0
        {
            "$addFields": {
                "non_recursive_supply": {
                    "$max": [
                        0,
                        {
                            "$subtract": [
                                {"$add": ["$i_token_sum", "$i_token_c_sum"]},
                                "$d_token_sum",
                            ]
                        },
                    ]
                }
            }
        },
        # Summarize total non-recursive supply across all accounts
        {
            "$group": {
                "_id": None,
                "total_non_recursive_supply": {"$sum": "$non_recursive_supply"},
            }
        },
    ]

    response = requests.post(
        QUERY_ENDPOINT,
        json={
            "dataSource": DATA_SOURCE,
            "database": DB,
            "collection": COLLECTION,
            "pipeline": pipeline,
        },
    )

    if response.status_code == 200:
        return int((response.json())["documents"][0]["total_non_recursive_supply"])
    else:
        print(f"Error: {response.status_code}")
        raise Exception(f"Error: {response.status_code}")


async def aggregate_stablecoins_non_recursive_supply(
    assets,
    dai_index,
    usdc_index,
    usdt_index,
    uno_index,
    dai_price,
    usdc_price,
    usdt_price,
    uno_price,
):
    QUERY_ENDPOINT = "https://us-east-2.aws.data.mongodb-api.com/app/data-yqlpb/endpoint/data/v1/action/aggregate"
    DATA_SOURCE = "nostra-production"
    DB = "prod-a-nostra-db"
    COLLECTION = "balances"

    stablecoin_addresses = {
        "DAI": {
            "i_token": "0x022ccca3a16c9ef0df7d56cbdccd8c4a6f98356dfd11abc61a112483b242db90",
            "i_token_c": "0x04f18ffc850cdfa223a530d7246d3c6fc12a5969e0aa5d4a88f470f5fe6c46e9",
            "d_token": "0x066037c083c33330a8460a65e4748ceec275bbf5f28aa71b686cbc0010e12597",
        },
        "USDC": {
            "i_token": "0x002fc2d4b41cc1f03d185e6681cbd40cced61915d4891517a042658d61cba3b1",
            "i_token_c": "0x05dcd26c25d9d8fd9fc860038dcb6e4d835e524eb8a85213a8cda5b7fff845f6",
            "d_token": "0x063d69ae657bd2f40337c39bf35a870ac27ddf91e6623c2f52529db4c1619a51",
        },
        "USDT": {
            "i_token": "0x0360f9786a6595137f84f2d6931aaec09ceec476a94a98dcad2bb092c6c06701",
            "i_token_c": "0x0453c4c996f1047d9370f824d68145bd5e7ce12d00437140ad02181e1d11dc83",
            "d_token": "0x024e9b0d6bc79e111e6872bb1ada2a874c25712cf08dfc5bcf0de008a7cca55f",
        },
        "UNO": {
            "i_token": "0x01325caf7c91ee415b8df721fb952fa88486a0fc250063eafddd5d3c67867ce7",
            "i_token_c": "0x02a3a9d7bcecc6d3121e3b6180b73c7e8f4c5f81c35a90c8dd457a70a842b723",
            "d_token": "0x04b036839a8769c04144cc47415c64b083a2b26e4a7daa53c07f6042a0d35792",
        },
    }

    # Flatten the addresses to a single list for the $in query operator
    token_addresses = [
        address for asset in stablecoin_addresses.values() for address in asset.values()
    ]

    pipeline = [
        {"$match": {"tokenAddress": {"$in": token_addresses}}},
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
                            {
                                "case": {
                                    "$in": [
                                        "$tokenAddress",
                                        [
                                            stablecoin_addresses["DAI"]["i_token"],
                                            stablecoin_addresses["USDC"]["i_token"],
                                            stablecoin_addresses["USDT"]["i_token"],
                                            stablecoin_addresses["UNO"]["i_token"],
                                        ],
                                    ]
                                },
                                "then": "i_token",
                            },
                            {
                                "case": {
                                    "$in": [
                                        "$tokenAddress",
                                        [
                                            stablecoin_addresses["DAI"]["i_token_c"],
                                            stablecoin_addresses["USDC"]["i_token_c"],
                                            stablecoin_addresses["USDT"]["i_token_c"],
                                            stablecoin_addresses["UNO"]["i_token_c"],
                                        ],
                                    ]
                                },
                                "then": "i_token_c",
                            },
                        ],
                        "default": "d_token",
                    }
                },
                "normalizedBalance": {
                    "$cond": {
                        "if": {"$eq": ["$asset", "USDT"]},
                        "then": {
                            "$multiply": [
                                {"$toDecimal": "$balanceWithoutIndex"},
                                "$usdt_index",
                                "$usdt_price",
                                1e12,
                            ]
                        },
                        "else": {
                            "$cond": {
                                "if": {"$eq": ["$asset", "USDC"]},
                                "then": {
                                    "$multiply": [
                                        {"$toDecimal": "$balanceWithoutIndex"},
                                        "$usdc_index",
                                        "$usdc_price",
                                        1e12,
                                    ]
                                },
                                "else": {
                                    "$cond": {
                                        "if": {"$eq": ["$asset", "DAI"]},
                                        "then": {
                                            "$multiply": [
                                                {"$toDecimal": "$balanceWithoutIndex"},
                                                "$dai_index",
                                                "$dai_price",
                                            ]
                                        },
                                        "else": {
                                            "$multiply": [
                                                {"$toDecimal": "$balanceWithoutIndex"},
                                                "$uno_index",
                                                "$uno_price",
                                            ]
                                        },
                                    }
                                },
                            }
                        },
                    }
                },
            }
        },
        {
            "$group": {
                "_id": "$accountAddress",
                "i_token_sum": {
                    "$sum": {
                        "$cond": [
                            {"$eq": ["$supplyType", "i_token"]},
                            "$normalizedBalance",
                            0,
                        ]
                    }
                },
                "i_token_c_sum": {
                    "$sum": {
                        "$cond": [
                            {"$eq": ["$supplyType", "i_token_c"]},
                            "$normalizedBalance",
                            0,
                        ]
                    }
                },
                "d_token_sum": {
                    "$sum": {
                        "$cond": [
                            {"$eq": ["$supplyType", "d_token"]},
                            "$normalizedBalance",
                            0,
                        ]
                    }
                },
            }
        },
        {
            "$addFields": {
                "non_recursive_supply": {
                    "$max": [
                        0,
                        {
                            "$subtract": [
                                {"$add": ["$i_token_sum", "$i_token_c_sum"]},
                                "$d_token_sum",
                            ]
                        },
                    ]
                }
            }
        },
        {
            "$group": {
                "_id": None,
                "total_non_recursive_supply": {"$sum": "$non_recursive_supply"},
            }
        },
    ]

    response = requests.post(
        QUERY_ENDPOINT,
        json={
            "dataSource": DATA_SOURCE,
            "database": DB,
            "collection": COLLECTION,
            "pipeline": pipeline,
        },
    )

    if response.status_code == 200:
        data = response.json()["documents"][0]
        return round(float(data["total_non_recursive_supply"]))
    else:
        print(f"Error: {response.status_code}")
        raise Exception(f"Error: {response.status_code}")


if __name__ == "__main__":
    # Record this so we can see how long it takes
    start_of_run = datetime.now(timezone.utc)

    # Get past data
    grant_scores_df = read_parquet_from_s3(BUCKET, S3_FILEPATH)

    # Assuming grant_scores_df['date'] has been defined and converted to datetime
    grant_scores_df["date"] = pd.to_datetime(grant_scores_df["date"], format="mixed")

    latest_date = grant_scores_df["date"].max()
    # latest_date = np.NaN
    # Use pd.isnull to check if latest_date is NaT
    if pd.isnull(latest_date):
        latest_date = datetime(2024, 3, 11)

    print_and_log(latest_date)

    # Calculate run_date as one day after the latest_date
    run_date = latest_date + timedelta(days=1)

    # Calculate next_date as one day after the run_date
    next_date = run_date + timedelta(days=1)

    # Print run_date
    print_and_log(run_date)

    # Check if run_date is today or later
    if run_date.date() >= datetime.now(timezone.utc).date():
        raise ValueError(
            "Need at least one full day of data to run! "
            + f"Run date is {run_date} and today is {datetime.now(timezone.utc).date()}"
        )

    print_and_log(f"{run_date} is a valid date")

    df = asyncio.run(main())

    df1 = grant_scores_df[grant_scores_df["date"] == grant_scores_df["date"].max()]

    # Sort DataFrames
    df = df[df.tokenSymbol.isin(['STRK', 'ETH', 'USDT', 'USDC','DAI', 'UNO', 'STB'])].sort_values(
        'tokenSymbol', ascending=True).reset_index(drop=True)
    df1 = df1[df1.tokenSymbol.isin(['STRK', 'ETH', 'USDT', 'USDC','DAI', 'UNO', 'STB'])].sort_values(
        'tokenSymbol', ascending=True).reset_index(drop=True)

    # Create lists
    stables_list = ['DAI', 'USDC', 'USDT', 'UNO']
    stables_list_stb = ['DAI', 'USDC', 'USDT', 'UNO', 'STB']
    non_stables_list = ['ETH', 'STRK', 'STB']
    non_stables_list_stb = ['ETH', 'STRK']

    prices_df = get_athena_prices_hourly(next_date)
    strk_prices_df = get_snowflake_strk_prices_hourly()
    uno_prices_df = get_athena_uno_prices_hourly()

    uno_prices_df = uno_prices_df[['timestamp', 'price']]
    uno_prices_df['symbol'] = 'UNO'

    # Assuming strk_prices_df is already defined and contains 'timestamp' in microseconds
    strk_prices_df['timestamp'] = pd.to_datetime(strk_prices_df['timestamp'], unit='us')
    uno_prices_df['timestamp'] = pd.to_datetime(uno_prices_df['timestamp'], unit='us')

    # Round timestamps to the nearest hour
    strk_prices_df['timestamp'] = strk_prices_df['timestamp'].dt.round('H')
    uno_prices_df['timestamp'] = uno_prices_df['timestamp'].dt.round('H')

    # Keep only the last hour 
    #strk_prices_df = strk_prices_df[strk_prices_df.timestamp==next_date]
    strk_prices_df = strk_prices_df[strk_prices_df.timestamp==strk_prices_df.timestamp.max()].drop_duplicates()
    #uno_prices_df = uno_prices_df[uno_prices_df.timestamp==next_date]
    uno_prices_df = uno_prices_df[uno_prices_df.timestamp==uno_prices_df.timestamp.max()].drop_duplicates()

    # Assuming prices_df is already defined and ready to be concatenated with strk_prices_df
    # Concatenate the dataframes
    prices_df = pd.concat([strk_prices_df, prices_df, uno_prices_df], ignore_index=True)

    # Add a row for STB (& UNO temporarily)
    nr1 = {'symbol': 'STB','timestamp' : next_date, 'price' : 1}
    #nr2 = {'symbol': 'UNO','timestamp' : pd.to_datetime('2024-03-15'), 'price' : 1}
    prices_df.loc[len(prices_df)] = nr1
    #prices_df.loc[len(prices_df)] = nr2

    # Verify that all prices are present
    assert (prices_df.shape[0] == 7)
    # Verify that all prices are recent (max 3 hours late from last day) - could use datetime.now() if we do max for all prices
    assert((prices_df['timestamp'].min()) > (next_date + timedelta(hours=-3)))

    # Merge decimals and prices
    token_data = {
        'tokenSymbol': ['ETH', 'USDT', 'USDC', 'STRK', 'DAI', 'UNO', 'STB'],
        'l1Address': ['0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2', '0xdac17f958d2ee523a2206206994597c13d831ec7', '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48', '0xCa14007Eff0dB1f8135f4C25B34De49AB0d42766','0x6b175474e89094c44da98b954eedeac495271d0f','0xnauno', '0xnastb'],
        'starknetAddressWith0s': ['0x049d36570d4e46f48e99674bd3fcc84644ddd6b96f7c741b1562b82f9e004dc7', '0x068f5c6a61780768455de69077e07e89787839bf8166decfbf92b645209c0fb8', '0x053c91253bc9682c04929ca02ed00b3e423f6710d2ee7e0d5ebb06f3ecf368a8', '0x04718f5a0fc34cc1af16a1cdee98ffb20c31f5cd61d6ab07201858f4287c938d', '0x00da114221cb83fa859dbdb4c44beeaa0bb37c7537ad5ae66fe5e0efd20e6eb3','0x0719b5092403233201aa822ce928bd4b551d0cdb071a724edd7dc5e5f57b7f34','0x0stable'],
        'starknetAddress': ['0x49d36570d4e46f48e99674bd3fcc84644ddd6b96f7c741b1562b82f9e004dc7', '0x68f5c6a61780768455de69077e07e89787839bf8166decfbf92b645209c0fb8', '0x53c91253bc9682c04929ca02ed00b3e423f6710d2ee7e0d5ebb06f3ecf368a8', '0x4718f5a0fc34cc1af16a1cdee98ffb20c31f5cd61d6ab07201858f4287c938d','0xda114221cb83fa859dbdb4c44beeaa0bb37c7537ad5ae66fe5e0efd20e6eb3','0x719b5092403233201aa822ce928bd4b551d0cdb071a724edd7dc5e5f57b7f34','0xstable'],
        'decimals': [18, 6, 6, 18, 18, 18, 1]
    }
    token_list_df = pd.DataFrame(token_data)

    final_balances = pd.merge(df, token_list_df[['starknetAddressWith0s', 'decimals']], left_on='market', right_on='starknetAddressWith0s', how='left').dropna().drop(columns='starknetAddressWith0s')
    final_balances = pd.merge(final_balances, prices_df, left_on='tokenSymbol', right_on='symbol', how='left').dropna().drop(columns=['symbol','timestamp'])
    final_balances = final_balances.sort_values('tokenSymbol', ascending=True).reset_index(drop=True)

    # Calculate Supplier Revenue and normalize token balances
    # final_balances['supply_token'] = final_balances['supply_token'] / 10 ** final_balances['decimals']
    # final_balances['borrow_token'] = final_balances['borrow_token'] / 10 ** final_balances['decimals']
    # final_balances['net_supply_token'] = final_balances['net_supply_token'] / 10 ** final_balances['decimals']
    # final_balances['non_recursive_supply_token'] = final_balances['non_recursive_supply_token'] / 10 ** final_balances['decimals']
    final_balances['non_recursive_supplier_revenue_total_token'] = ((final_balances['lending_index_rate'] / df1['lending_index_rate']) - 1) * final_balances['non_recursive_supply_token']

    # Calculate USD equivalent values
    final_balances['supply'] = final_balances['supply_token'] * final_balances['price']
    final_balances['borrow'] = final_balances['borrow_token'] * final_balances['price']
    final_balances['net_supply'] = final_balances['supply'] - final_balances['borrow'] 
    final_balances['non_recursive_supply'] = final_balances['non_recursive_supply_token'] * final_balances['price']
    final_balances['non_recursive_supplier_revenue_total'] = final_balances['non_recursive_supplier_revenue_total_token'] * final_balances['price']
    final_balances['etl_timestamp'] = (datetime.now(timezone.utc)).strftime("%Y-%m-%d %H:%M:%S")
    final_balances.loc[final_balances.tokenSymbol =='STB','non_recursive_supplier_revenue_total'] = final_balances.loc[final_balances.tokenSymbol.isin(stables_list),'non_recursive_supplier_revenue_total'].sum()
    final_balances.loc[final_balances.tokenSymbol =='STB','non_recursive_supplier_revenue_total_token'] = final_balances.loc[final_balances.tokenSymbol =='STB','non_recursive_supplier_revenue_total'].copy()
    protocol_scores_final = final_balances.drop(columns=['decimals']).sort_values('tokenSymbol', ascending=True).reset_index(drop=True)

    # Verify that all protocol scores are present
    assert (protocol_scores_final.shape[0] == 7)

    # Check results
    # First Order Check
    assert ((protocol_scores_final)["supply"] >= 0).all()
    assert ((protocol_scores_final)["borrow"] >= 0).all()
    assert ((protocol_scores_final)["net_supply"] >= 0).all()
    assert ((protocol_scores_final)["non_recursive_supply"] >= 0).all()
    assert ((protocol_scores_final)["non_recursive_supplier_revenue_total"] >= 0).all()
    assert ((protocol_scores_final)["price"] >= 0).all()
    # Second Order Check
    assert ((protocol_scores_final)["supply"] > (protocol_scores_final)["borrow"]).all()
    assert (
        (protocol_scores_final)["non_recursive_supply"]
        >= (protocol_scores_final)["net_supply"]
    ).all()

    # Need to re-set grant_scores_df in case it has changed
    grant_scores_df = read_parquet_from_s3(BUCKET, S3_FILEPATH)

    # Filter out past runs on the current date to avoid dupes
    grant_scores_df["date"] = pd.to_datetime(grant_scores_df["date"], format="mixed")
    filtered_grant_scores_df = grant_scores_df[grant_scores_df["date"] != run_date]

    print_and_log(f"Rows filtered out: {len(grant_scores_df) - len(filtered_grant_scores_df)}")

    protocol_scores_final["date"] = protocol_scores_final["date"].astype(str)
    filtered_grant_scores_df["date"] = filtered_grant_scores_df["date"].astype(str)

    # Convert 'date' column in combined_output_df to string format to avoid error
    protocol_scores_final["etl_timestamp"] = (datetime.now(timezone.utc)).strftime(
        "%Y-%m-%d %H:%M:%S"
    )

    combined_output_df = pd.concat(
        [filtered_grant_scores_df, protocol_scores_final], ignore_index=True
    )

    combined_output_df['protocol'] = combined_output_df['protocol'].astype(str)
    combined_output_df['date'] = combined_output_df['date'].astype(str)
    combined_output_df['market'] = combined_output_df['market'].astype(str)
    combined_output_df['tokenSymbol'] = combined_output_df['tokenSymbol'].astype(str)
    combined_output_df['supply'] = combined_output_df['supply'].astype(float)
    combined_output_df['borrow'] = combined_output_df['borrow'].astype(float)
    combined_output_df['net_supply'] = combined_output_df['net_supply'].astype(float)
    combined_output_df['non_recursive_supply'] = combined_output_df['non_recursive_supply'].astype(float)
    combined_output_df['non_recursive_supplier_revenue_total'] = combined_output_df['non_recursive_supplier_revenue_total'].astype(float)
    combined_output_df['supply_token'] = combined_output_df['supply_token'].astype(float)
    combined_output_df['borrow_token'] = combined_output_df['borrow_token'].astype(float)
    combined_output_df['net_supply_token'] = combined_output_df['net_supply_token'].astype(float)
    combined_output_df['non_recursive_supply_token'] = combined_output_df['non_recursive_supply_token'].astype(float)
    combined_output_df['non_recursive_supplier_revenue_total_token'] = combined_output_df['non_recursive_supplier_revenue_total_token'].astype(float)
    combined_output_df['block_height'] = combined_output_df['block_height'].astype(int)
    combined_output_df['lending_index_rate'] = combined_output_df['lending_index_rate'].astype(float)
    combined_output_df['price'] = combined_output_df['price'].astype(float)
    combined_output_df['etl_timestamp'] = combined_output_df['etl_timestamp'].astype(str)

    print_and_log(f"Rows added: {len(protocol_scores_final)}")

    if write_to_s3:
        # Write to S3 table
        load_parquet_to_s3(BUCKET, S3_FILEPATH, combined_output_df)
        print_and_log(f"grant_scores_{PROTOCOL}.parquet written to s3.")
    else:
        print_and_log(f"Skipping writing grant_scores_{PROTOCOL}.parquet to s3.")

    time_to_run = datetime.now(timezone.utc) - start_of_run

    print_and_log(f"Time to run: {time_to_run}")

    
