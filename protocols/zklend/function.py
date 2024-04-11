import os
os.environ["AWS_DEFAULT_REGION"] = "us-east-1"

import sys
sys.path.append(".")

import asyncio
import pandas as pd
import json
import aiohttp

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Dict, List, Union
from starknet_py.contract import Contract
from starknet_py.net.full_node_client import FullNodeClient

from utils.s3_utils import load_parquet_to_s3, read_parquet_from_s3
from utils.logging_utils import print_and_log
from utils.athena_utils import get_athena_prices_hourly
from utils.snowflake_utils import get_snowflake_strk_prices_hourly

# Set this False if just testing
write_to_s3 = True

PROTOCOL = "zklend"
BUCKET = "starknet-openblocklabs"
S3_FILEPATH = f"grant_scores_lending_test/grant_scores_lending_{PROTOCOL}.parquet"

Z_TOKEN_ABI: List = json.load(open("./protocols/zklend/ztoken.abi.json"))
MARKET_ABI: List = json.load(open("./protocols/zklend/market.abi.json"))
PRAGMA_ADAPTER_ABI: List = json.load(open("./protocols/zklend/pragma_adapter.abi.json"))

NODE_URL = "https://starknet-mainnet.public.blastapi.io"
SUBGRAPH_URL = "https://hk-gateway.query.graph.zklend.com/subgraphs/name/zklend/web"

MARKET = "0x04c0a5193d58f74fbace4b74dcf65481e734ed1714121bdc571da345540efa05"

PAGINATION_SIZE = 1000

ACCUMULATOR_DECIMALS = 27
# The number of decimal places used for USDT, USDC, and DAI at Pragma is all 8.
PRAGMA_STABLECOIN_PRICE_DECIMALS = 8

non_stables: List[Dict[str, Union[str, int]]] = [
    {
        "symbol": "STRK",
        "token_decimals": 18,
        "underlying": "0x04718f5a0fc34cc1af16a1cdee98ffb20c31f5cd61d6ab07201858f4287c938d",
        "z_token": "0x06d8fa671ef84f791b7f601fa79fea8f6ceb70b5fa84189e3159d532162efc21",
    },
    {
        "symbol": "ETH",
        "token_decimals": 18,
        "underlying": "0x049d36570d4e46f48e99674bd3fcc84644ddd6b96f7c741b1562b82f9e004dc7",
        "z_token": "0x01b5bd713e72fdc5d63ffd83762f81297f6175a5e0a4771cdadbc1dd5fe72cb1",
    },
]
stables: List[Dict[str, Union[str, int]]] = [
    {
        "symbol": "USDC",
        "token_decimals": 6,
        "underlying": "0x053c91253bc9682c04929ca02ed00b3e423f6710d2ee7e0d5ebb06f3ecf368a8",
        "z_token": "0x047ad51726d891f972e74e4ad858a261b43869f7126ce7436ee0b2529a98f486",
        "pragma_price_adapter": "0x065354c0aefe9855866ef8f6215452a83dc3cebcf0100e22374c7da55f76f9b2",
    },
    {
        "symbol": "USDT",
        "token_decimals": 6,
        "underlying": "0x068f5c6a61780768455de69077e07e89787839bf8166decfbf92b645209c0fb8",
        "z_token": "0x00811d8da5dc8a2206ea7fd0b28627c2d77280a515126e62baa4d78e22714c4a",
        "pragma_price_adapter": "0x060f407449b26bc5e83461595e25dff3cca0733f654cd92a29bdd397d24e25bf",
    },
    {
        "symbol": "DAI",
        "token_decimals": 18,
        "underlying": "0x00da114221cb83fa859dbdb4c44beeaa0bb37c7537ad5ae66fe5e0efd20e6eb3",
        "z_token": "0x062fa7afe1ca2992f8d8015385a279f49fad36299754fb1e9866f4f052289376",
        "pragma_price_adapter": "0x05d1bc06ca368cc451f63b20bc12bd2299a4ae7776f4dcf977723839bef311a0",
    },
]
stable_symbols = [asset["symbol"] for asset in stables]

client = FullNodeClient(node_url=NODE_URL)

today = datetime.now().strftime("%Y-%m-%d")


async def get_supply(z_token_address: int, block_number: int) -> int:
    contract = Contract(
        address=z_token_address,
        abi=Z_TOKEN_ABI,
        provider=client,
    )
    (uint_face_value,) = await contract.functions["totalSupply"].call(
        block_number=block_number
    )
    return uint_face_value


async def get_debt(underlying_address: int, block_number: int) -> int:
    contract = Contract(
        address=MARKET,
        abi=MARKET_ABI,
        provider=client,
    )
    (uint_face_value,) = await contract.functions["get_total_debt_for_token"].call(
        underlying_address, block_number=block_number
    )
    return uint_face_value


async def get_lending_accumulator(underlying_address: int, block_number: int) -> int:
    contract = Contract(
        address=MARKET,
        abi=MARKET_ABI,
        provider=client,
    )
    (uint_value,) = await contract.functions["get_lending_accumulator"].call(
        underlying_address, block_number=block_number
    )
    return uint_value


async def get_debt_accumulator(underlying_address: int, block_number: int) -> int:
    contract = Contract(
        address=MARKET,
        abi=MARKET_ABI,
        provider=client,
    )
    (uint_value,) = await contract.functions["get_debt_accumulator"].call(
        underlying_address, block_number=block_number
    )
    return uint_value


async def get_pragma_price(adapter_address: int, block_number: int) -> int:
    contract = Contract(
        address=adapter_address,
        abi=PRAGMA_ADAPTER_ABI,
        provider=client,
    )
    (uint_value,) = await contract.functions["get_price"].call(
        block_number=block_number
    )
    return uint_value


# This function iterates through all the raw balances and debts per token for all users,
# so it will take a while to run.
async def get_raw_balance_per_user(
    underlying_address: str, block_number: int
) -> Dict[str, Dict[str, int]]:
    raw_balance_per_user = {}
    skip = 0

    async def send_request(session, payload):
        nonlocal raw_balance_per_user
        nonlocal skip
        async with session.post(
            SUBGRAPH_URL, json=payload, headers={"Content-Type": "application/json"}
        ) as response:
            if response.status == 200:
                data = await response.json()
                if not "data" in data:
                    print(f'Error: no "data" in {data}. Retrying...')
                    await asyncio.sleep(5)
                    return False
                raw_supply_balances = data["data"]["ztokenRawBalances"]
                raw_debt_balances = data["data"]["userRawDebts"]

                if len(raw_supply_balances) == 0 and len(raw_debt_balances) == 0:
                    return True
                skip += PAGINATION_SIZE

                for item in raw_supply_balances:
                    raw_supply = item["raw_balance"]
                    if item["user"] in raw_balance_per_user:
                        raw_balance_per_user[item["user"]]["supply"] = int(raw_supply)
                    else:
                        raw_balance_per_user[item["user"]] = {
                            "supply": int(raw_supply),
                            "debt": 0,
                        }
                for item in raw_debt_balances:
                    raw_debt = item["amount"]
                    if item["user"] in raw_balance_per_user:
                        raw_balance_per_user[item["user"]]["debt"] = int(raw_debt)
                    else:
                        raw_balance_per_user[item["user"]] = {
                            "supply": 0,
                            "debt": int(raw_debt),
                        }

                print(f"len(raw_balance_per_user): {len(raw_balance_per_user)}")
            else:
                print(f"Error: {response.status_code}.  Retrying...")
                await asyncio.sleep(5)
                return False

    async with aiohttp.ClientSession() as session:
        while True:
            payload = {
                "query": f"""{{
                    ztokenRawBalances(
                        where:{{
                            token: "{underlying_address}",
                        }}, 
                        first: {PAGINATION_SIZE},
                        skip: {skip},
                        block: {{
                            number: {block_number}
                        }}
                    ) {{
                        user
                        raw_balance
                    }}
                    userRawDebts(
                        where:{{
                            token: "{underlying_address}",
                        }}, 
                        first: {PAGINATION_SIZE},
                        skip: {skip},
                        block: {{
                            number: {block_number}
                        }}
                    ) {{
                        user
                        amount
                    }}
                }}"""
            }
            done = await send_request(session, payload)
            if done:
                break

    return raw_balance_per_user


def calc_non_recursive_supply(
    raw_balance_per_user: Dict[str, Dict[str, int]],
    lending_accumulator: int,
    debt_accumulator: int,
):
    total_non_recursive_supply = Decimal(0)
    for _, balances in raw_balance_per_user.items():
        uint_face_value_supply = Decimal(balances["supply"]) * Decimal(
            lending_accumulator
        )
        face_value_supply = scale_down(uint_face_value_supply, ACCUMULATOR_DECIMALS)
        uint_face_value_debt = Decimal(balances["debt"]) * Decimal(debt_accumulator)
        face_value_debt = scale_down(uint_face_value_debt, ACCUMULATOR_DECIMALS)
        non_recursive_supply_per_user = face_value_supply - face_value_debt
        # If positive it adds to the cumulative sum, if negative it adds 0
        if non_recursive_supply_per_user > 0:
            total_non_recursive_supply += non_recursive_supply_per_user

    return total_non_recursive_supply


def scale_down(value: int, decimals: int) -> Decimal:
    return Decimal(value) / Decimal(f"1e{decimals}")


def remove_keys(obj, keys):
    return {key: value for key, value in obj.items() if key not in keys}


async def get_data(asset, block_height):
    z_token_int = int(asset["z_token"], 16)
    underlying_int = int(asset["underlying"], 16)

    lending_accumulator = await get_lending_accumulator(underlying_int, block_height)
    debt_accumulator = await get_debt_accumulator(underlying_int, block_height)
    raw_balance_per_user = await get_raw_balance_per_user(
        asset["underlying"], block_height
    )
    total_non_recursive_supply = calc_non_recursive_supply(
        raw_balance_per_user, lending_accumulator, debt_accumulator
    )

    supply = await get_supply(z_token_int, block_height)
    debt = await get_debt(underlying_int, block_height)

    decimal_price = None
    if "pragma_price_adapter" in asset:
        pragma_price_adapter_int = int(asset["pragma_price_adapter"], 16)
        uint_raw_price = await get_pragma_price(pragma_price_adapter_int, block_height)
        decimal_price = scale_down(uint_raw_price, PRAGMA_STABLECOIN_PRICE_DECIMALS)

    return {
        "protocol": "zkLend",
        "date": today,
        "market": asset["underlying"],
        "tokenSymbol": asset["symbol"],
        "supply_token": scale_down(supply, asset["token_decimals"]),
        "borrow_token": -scale_down(debt, asset["token_decimals"]),
        "net_supply_token": scale_down(supply - debt, asset["token_decimals"]),
        "non_recursive_supply_token": scale_down(
            total_non_recursive_supply, asset["token_decimals"]
        ),
        "block_height": block_height,
        "lending_index_rate": scale_down(lending_accumulator, ACCUMULATOR_DECIMALS),
        "raw_balance_per_user": raw_balance_per_user,
        "lending_accumulator": lending_accumulator,
        "debt_accumulator": debt_accumulator,
        "token_decimals": asset["token_decimals"],
        "decimal_price": decimal_price,
    }


def get_all_stables_data(assets, block_height):
    stables_raw_balance_per_user = {}
    stables_supply = 0
    stables_debt = 0

    for asset in assets:
        decimal_price = asset["decimal_price"]
        lending_accumulator = asset["lending_accumulator"]
        debt_accumulator = asset["debt_accumulator"]
        for user, balances in asset["raw_balance_per_user"].items():
            uint_face_value_supply = Decimal(balances["supply"]) * Decimal(
                lending_accumulator
            )
            face_value_supply = scale_down(uint_face_value_supply, ACCUMULATOR_DECIMALS)
            uint_face_value_debt = Decimal(balances["debt"]) * Decimal(debt_accumulator)
            face_value_debt = scale_down(uint_face_value_debt, ACCUMULATOR_DECIMALS)

            scaled_face_value_supply = scale_down(
                face_value_supply, asset["token_decimals"]
            )
            scaled_face_value_debt = scale_down(
                face_value_debt, asset["token_decimals"]
            )
            priced_scaled_face_value_supply = scaled_face_value_supply * decimal_price
            priced_scaled_face_value_debt = scaled_face_value_debt * decimal_price

            if user in stables_raw_balance_per_user:
                stables_raw_balance_per_user[user][
                    "supply"
                ] += priced_scaled_face_value_supply
                stables_raw_balance_per_user[user][
                    "debt"
                ] += priced_scaled_face_value_debt
            else:
                stables_raw_balance_per_user[user] = {
                    "supply": priced_scaled_face_value_supply,
                    "debt": priced_scaled_face_value_debt,
                }

        stables_supply += asset["supply_token"] * decimal_price
        # asset["borrow_token"] is already negative
        stables_debt += asset["borrow_token"] * decimal_price

    stables_total_non_recursive_supply = Decimal(0)
    for _, balances in stables_raw_balance_per_user.items():
        non_recursive_supply_per_user = balances["supply"] - balances["debt"]
        # If positive it adds to the cumulative sum, if negative it adds 0
        if non_recursive_supply_per_user > 0:
            stables_total_non_recursive_supply += non_recursive_supply_per_user

    return {
        "protocol": "zkLend",
        "date": today,
        "market": "0x0stable",
        "tokenSymbol": "STB",
        "supply_token": stables_supply,
        "borrow_token": stables_debt,
        # stables_debt is already negative
        "net_supply_token": stables_supply + stables_debt,
        "non_recursive_supply_token": stables_total_non_recursive_supply,
        "block_height": block_height,
        "lending_index_rate": 1,
    }


async def main():
    block_height = await client.get_block_number()

    data = [get_data(asset, block_height) for asset in [*non_stables, *stables]]
    result = await asyncio.gather(*data)
    non_stables_result = []
    stables_result = []
    for asset in result:
        if asset["tokenSymbol"] in stable_symbols:
            stables_result.append(asset)
        else:
            non_stables_result.append(asset)
    all_stables_result = get_all_stables_data(stables_result, block_height)

    keys_to_remove = [
        "raw_balance_per_user",
        "lending_accumulator",
        "debt_accumulator",
        "token_decimals",
        "decimal_price",
    ]
    cleaned = list(
        map(
            lambda obj: remove_keys(obj, keys_to_remove),
            [*non_stables_result, *stables_result],
        )
    )
    everything = [*cleaned, all_stables_result]

    df = pd.DataFrame(everything)

    return df


if __name__ == "__main__":

    # Record this so we can see how long it takes
    start_of_run = datetime.now(timezone.utc)

    # get past data
    grant_scores_df = read_parquet_from_s3(BUCKET, S3_FILEPATH)

    grant_scores_df["date"] = pd.to_datetime(grant_scores_df["date"], format="mixed")

    latest_date = grant_scores_df["date"].max()
    # latest_date = np.NaN
    # Use pd.isnull to check if latest_date is NaT
    if pd.isnull(latest_date):
        latest_date = datetime(2024, 3, 11)

    print_and_log(f"Latest date: {latest_date}")

    # Calculate run_date as one day after the latest_date
    run_date = latest_date + timedelta(days=1)

    # Calculate next_date as one day after the run_date
    next_date = run_date + timedelta(days=1)

    print_and_log(f"Run date: {run_date}")

    # Check if run_date is today or later
    if run_date.date() >= datetime.now(timezone.utc).date():
        raise ValueError(
            "Need at least one full day of data to run! "
            + f"Run date is {run_date} and today is {datetime.now(timezone.utc).date()}"
        )

    print_and_log(f"{run_date} is a valid date")

    # run main to get new data
    df = asyncio.run(main())

    # Get supply index rate for each token from last day
    df1 = grant_scores_df[grant_scores_df["date"] == grant_scores_df["date"].max()]

    # Sort DataFrames
    df = (
        df[df.tokenSymbol.isin(["STRK", "ETH", "USDT", "USDC"])]
        .sort_values("tokenSymbol", ascending=True)
        .reset_index(drop=True)
    )
    df1 = (
        df1[df1.tokenSymbol.isin(["STRK", "ETH", "USDT", "USDC"])]
        .sort_values("tokenSymbol", ascending=True)
        .reset_index(drop=True)
    )

    prices_df = get_athena_prices_hourly()
    strk_prices_df = get_snowflake_strk_prices_hourly()

    # Assuming strk_prices_df is already defined and contains 'timestamp' in microseconds
    strk_prices_df["timestamp"] = pd.to_datetime(strk_prices_df["timestamp"], unit="us")

    # Round timestamps to the nearest hour
    strk_prices_df["timestamp"] = strk_prices_df["timestamp"].dt.round("H")

    # Keep only the last hour
    # strk_prices_df = strk_prices_df[strk_prices_df.timestamp==next_date]
    strk_prices_df = strk_prices_df[
        strk_prices_df.timestamp == strk_prices_df.timestamp.max()
    ]
    # Assuming prices_df is already defined and ready to be concatenated with strk_prices_df
    # Concatenate the dataframes
    prices_df = pd.concat([strk_prices_df, prices_df])

    # Verify that all prices are present
    assert prices_df.shape[0] == 4

    # Merge decimals and prices
    token_data = {
        "tokenSymbol": ["ETH", "USDT", "USDC", "STRK"],
        "l1Address": [
            "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
            "0xdac17f958d2ee523a2206206994597c13d831ec7",
            "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
            "0xCa14007Eff0dB1f8135f4C25B34De49AB0d42766",
        ],
        "starknetAddressWith0s": [
            "0x049d36570d4e46f48e99674bd3fcc84644ddd6b96f7c741b1562b82f9e004dc7",
            "0x068f5c6a61780768455de69077e07e89787839bf8166decfbf92b645209c0fb8",
            "0x053c91253bc9682c04929ca02ed00b3e423f6710d2ee7e0d5ebb06f3ecf368a8",
            "0x04718f5a0fc34cc1af16a1cdee98ffb20c31f5cd61d6ab07201858f4287c938d",
        ],
        "starknetAddress": [
            "0x49d36570d4e46f48e99674bd3fcc84644ddd6b96f7c741b1562b82f9e004dc7",
            "0x68f5c6a61780768455de69077e07e89787839bf8166decfbf92b645209c0fb8",
            "0x53c91253bc9682c04929ca02ed00b3e423f6710d2ee7e0d5ebb06f3ecf368a8",
            "0x4718f5a0fc34cc1af16a1cdee98ffb20c31f5cd61d6ab07201858f4287c938d",
        ],
        "decimals": [18, 6, 6, 18],
    }
    token_list_df = pd.DataFrame(token_data)

    final_balances = (
        pd.merge(
            df,
            token_list_df[["starknetAddressWith0s", "decimals"]],
            left_on="market",
            right_on="starknetAddressWith0s",
            how="left",
        )
        .dropna()
        .drop(columns="starknetAddressWith0s")
    )
    final_balances = (
        pd.merge(
            final_balances,
            prices_df,
            left_on="tokenSymbol",
            right_on="symbol",
            how="left",
        )
        .dropna()
        .drop(columns=["symbol", "timestamp"])
    )
    final_balances = final_balances.sort_values(
        "tokenSymbol", ascending=True
    ).reset_index(drop=True)

    # Calculate Supplier Revenue and normalize token balances
    final_balances['lending_index_rate'] = final_balances['lending_index_rate'].astype(float)
    final_balances['supply_token'] = final_balances['supply_token'].astype(float)
    final_balances['borrow_token'] = -final_balances['borrow_token'].astype(float)
    final_balances['net_supply_token'] = final_balances['net_supply_token'].astype(float)
    final_balances['non_recursive_supply_token'] = final_balances['non_recursive_supply_token'].astype(float)
    final_balances['non_recursive_supplier_revenue_total_token'] = ((final_balances['lending_index_rate'] / df1['lending_index_rate']) - 1) * final_balances['non_recursive_supply_token']
    final_balances['non_recursive_supplier_revenue_total_token'] = final_balances['non_recursive_supplier_revenue_total_token'].astype(float)

    # Calculate USD equivalent values
    final_balances['supply'] = final_balances['supply_token'] * final_balances['price']
    final_balances['borrow'] = final_balances['borrow_token'] * final_balances['price']
    final_balances['net_supply'] = final_balances['supply'] - final_balances['borrow'] 
    final_balances['non_recursive_supply'] = final_balances['non_recursive_supply_token'] * final_balances['price']
    final_balances['non_recursive_supplier_revenue_total'] = final_balances['non_recursive_supplier_revenue_total_token'] * final_balances['price']
    final_balances['etl_timestamp'] = (datetime.now(timezone.utc)).strftime("%Y-%m-%d %H:%M:%S")
    protocol_scores_final = final_balances.drop(columns=['decimals']).sort_values('tokenSymbol', ascending=True).reset_index(drop=True)

    # Verify that all protocol scores are present
    assert (protocol_scores_final.shape[0] == 4)

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

    
