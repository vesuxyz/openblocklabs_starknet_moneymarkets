import os
os.environ["AWS_DEFAULT_REGION"] = "us-east-1"

import sys
sys.path.append(".")

import asyncio
import pandas as pd
from datetime import datetime, timedelta, timezone
from borrow.borrow import main as borrow_main
from lend.lend import main as lend_main

from utils.s3_utils import load_parquet_to_s3, read_parquet_from_s3
from utils.athena_utils import get_athena_prices_hourly
from utils.logging_utils import print_and_log
from utils.snowflake_utils import get_snowflake_strk_prices_hourly

PROTOCOL = "nimbora"
BUCKET = "starknet-openblocklabs"
S3_FILEPATH = f"grant_scores_lending_test/grant_scores_lending_{PROTOCOL}.parquet"

# Set this False if just testing
write_to_s3 = True

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

    df0 = asyncio.run(borrow_main())
    df01 = asyncio.run(lend_main())

    df = (
        df01.groupby(["protocol", "date", "market", "tokenSymbol", "block_height"])
        .sum()
        .reset_index()
    )
    df = (
        df[df.tokenSymbol.isin(["STRK", "ETH", "USDT", "USDC"])]
        .sort_values("tokenSymbol", ascending=True)
        .reset_index(drop=True)
    )
    df["lending_index_rate"] = 1.0

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

    prices_df = get_athena_prices_hourly(next_date)    
    strk_prices_df = get_snowflake_strk_prices_hourly()

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
    #final_balances['supply_token'] = final_balances['supply_token'] / 10 ** final_balances['decimals']
    #final_balances['borrow_token'] = final_balances['borrow_token'] / 10 ** final_balances['decimals']
    #final_balances['net_supply_token'] = final_balances['net_supply_token'] / 10 ** final_balances['decimals']
    #final_balances['non_recursive_supply_token'] = final_balances['non_recursive_supply_token'] / 10 ** final_balances['decimals']
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
    assert (protocol_scores_final.shape[0] == 1)

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