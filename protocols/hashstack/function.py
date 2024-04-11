import os
os.environ["AWS_DEFAULT_REGION"] = "us-east-1"

import sys
sys.path.append(".")

import asyncio, json
from starknet_py.contract import Contract
from starknet_py.net.full_node_client import FullNodeClient
from datetime import datetime, timedelta, timezone
import pandas as pd

from utils.s3_utils import load_parquet_to_s3, read_parquet_from_s3
from utils.athena_utils import get_athena_prices_hourly
from utils.logging_utils import print_and_log
from utils.snowflake_utils import get_snowflake_strk_prices_hourly

# Set this False if just testing
write_to_s3 = True

PROTOCOL = "hashstack"
BUCKET = "starknet-openblocklabs"
S3_FILEPATH = f"grant_scores_lending_test/grant_scores_lending_{PROTOCOL}.parquet"

rTokenAbi = json.load(open("./protocols/hashstack/rToken.abi.json"))
dTokenAbi = json.load(open("./protocols/hashstack/dToken.abi.json"))

ORACLE_ADDRESS = "0x683852789848dea686fcfb66aaebf6477d83b25d8894aae73b15ff19b765bf0"


# Returns row for each token
# Contains supply and lending index
async def get_token_info(tokenInfo, provider):
    supply_contract = Contract(
        address=tokenInfo["rToken"], abi=rTokenAbi, provider=provider
    )

    (total_assets,) = await supply_contract.functions["total_assets"].call()

    (_, lending_rate) = await supply_contract.functions["exchange_rate"].call()

    borrow_contract = Contract(
        address=tokenInfo["dToken"], abi=dTokenAbi, provider=provider
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
        "supply_token": total_assets / 10 ** tokenInfo["decimals"],
        "borrow_token": total_debt / 10 ** tokenInfo["decimals"],
        "net_supply_token": net_supply / 10 ** tokenInfo["decimals"],
        "non_recursive_supply_token": total_assets / 10 ** tokenInfo["decimals"],
        "block_height": block,
        "lending_index_rate": lending_rate / 10**18,
    }


def normalize(value, decimals):
    return value / (10**decimals)


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
    stables = ["USDC", "USDT", "DAI"]
    eth_price = normalize(await get_pragma_eth_price(client), 18)
    usdt_price = (
        normalize(
            await get_pragma_price(
                "0x068f5c6a61780768455de69077e07e89787839bf8166decfbf92b645209c0fb8",
                client,
            ),
            18,
        )
        * eth_price
    )
    usdc_price = (
        normalize(
            await get_pragma_price(
                "0x053c91253bc9682c04929ca02ed00b3e423f6710d2ee7e0d5ebb06f3ecf368a8",
                client,
            ),
            18,
        )
        * eth_price
    )
    dai_price = (
        normalize(
            await get_pragma_price(
                "0x00da114221cb83fa859dbdb4c44beeaa0bb37c7537ad5ae66fe5e0efd20e6eb3",
                client,
            ),
            18,
        )
        * eth_price
    )

    print(eth_price, usdc_price, usdt_price, dai_price)
    prices = {"DAI": dai_price, "USDC": usdc_price, "USDT": usdt_price}
    return {
        "protocol": "Hashstack",
        "date": data[0]["date"],
        "market": "0x0stable",
        "tokenSymbol": "STB",
        "supply_token": sum(
            [
                row["supply_token"] * prices[row["tokenSymbol"]]
                for row in data
                if row["tokenSymbol"] in stables
            ]
        ),
        "borrow_token": sum(
            [
                row["borrow_token"] * prices[row["tokenSymbol"]]
                for row in data
                if row["tokenSymbol"] in stables
            ]
        ),
        "net_supply_token": sum(
            [
                row["net_supply_token"] * prices[row["tokenSymbol"]]
                for row in data
                if row["tokenSymbol"] in stables
            ]
        ),
        "non_recursive_supply_token": sum(
            [
                row["non_recursive_supply_token"] * prices[row["tokenSymbol"]]
                for row in data
                if row["tokenSymbol"] in stables
            ]
        ),
        "block_height": data[0]["block_height"],
        "lending_index_rate": 1,
    }


# Define functions
async def main():
    """
    Supply your calculation here according to the Guidelines.
    """

    node_url = "https://starknet-mainnet.public.blastapi.io"
    provider = FullNodeClient(node_url=node_url)

    with open("./protocols/hashstack/tokens.json", "r") as f:
        tokens = json.load(f)
        coroutines = [get_token_info(tokenInfo, provider) for tokenInfo in tokens]
        gathered_results = await asyncio.gather(*coroutines)

        stables_combined_row = await combine_stables(gathered_results, provider)
        gathered_results.append(stables_combined_row)

        df = pd.DataFrame(gathered_results)

    return df


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

    # Get supply index rate for each token from last day
    df1 = grant_scores_df[grant_scores_df["date"] == grant_scores_df["date"].max()]

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

    # Merge decimals and prices / changed $strk on starknetAddress for Hashstack
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
            "0x04718f5a0fc34cc1af16a1cdee98ffb20c31f5cd61d6ab07201858f4287c938d",
        ],
        "decimals": [18, 6, 6, 18],
    }
    token_list_df = pd.DataFrame(token_data)

    final_balances = (
        pd.merge(
            df,
            token_list_df[["starknetAddress", "decimals"]],
            left_on="market",
            right_on="starknetAddress",
            how="left",
        )
        .dropna()
        .drop(columns="starknetAddress")
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
    final_balances['supply_token'] = final_balances['supply_token'] / 10 ** final_balances['decimals']
    final_balances['borrow_token'] = final_balances['borrow_token'] / 10 ** final_balances['decimals']
    final_balances['net_supply_token'] = final_balances['net_supply_token'] / 10 ** final_balances['decimals']
    final_balances['non_recursive_supply_token'] = final_balances['non_recursive_supply_token'] / 10 ** final_balances['decimals']
    final_balances['non_recursive_supplier_revenue_total_token'] = ((final_balances['lending_index_rate'] / df1['lending_index_rate']) - 1) * final_balances['non_recursive_supply_token']

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


    
