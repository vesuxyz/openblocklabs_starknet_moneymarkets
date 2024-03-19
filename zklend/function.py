import asyncio
import pandas as pd
import json
import aiohttp
from datetime import datetime
from decimal import Decimal
from typing import Dict, List, Union
from starknet_py.contract import Contract
from starknet_py.net.full_node_client import FullNodeClient


Z_TOKEN_ABI: List = json.load(open("./zklend/ztoken.abi.json"))
MARKET_ABI: List = json.load(open("./zklend/market.abi.json"))

NODE_URL = "https://starknet-mainnet.public.blastapi.io"
SUBGRAPH_URL = "https://hk-gateway.query.graph.zklend.com/subgraphs/name/zklend/web"

MARKET = "0x04c0a5193d58f74fbace4b74dcf65481e734ed1714121bdc571da345540efa05"

PAGINATION_SIZE = 1000

ACCUMULATOR_DECIMALS = 27

assets: List[Dict[str, Union[str, int]]] = [
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
    {
        "symbol": "USDC",
        "token_decimals": 6,
        "underlying": "0x053c91253bc9682c04929ca02ed00b3e423f6710d2ee7e0d5ebb06f3ecf368a8",
        "z_token": "0x047ad51726d891f972e74e4ad858a261b43869f7126ce7436ee0b2529a98f486",
    },
    {
        "symbol": "USDT",
        "token_decimals": 6,
        "underlying": "0x068f5c6a61780768455de69077e07e89787839bf8166decfbf92b645209c0fb8",
        "z_token": "0x00811d8da5dc8a2206ea7fd0b28627c2d77280a515126e62baa4d78e22714c4a",
    },
]

client = FullNodeClient(node_url=NODE_URL)


def get_today() -> str:
    formatted_date = datetime.now().strftime("%Y-%m-%d")
    return formatted_date


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


# This function iterates through all the raw balances and debts per token for all users,
# so it will take a while to run.
async def get_raw_balance_per_user(
    underlying_address: str, block_number: int
) -> Dict[str, Dict[str, int]]:
    raw_balance_per_user = {}
    skip = 0
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
            async with session.post(
                SUBGRAPH_URL, json=payload, headers={"Content-Type": "application/json"}
            ) as response:
                if response.status == 200:
                    data = await response.json()
                    raw_supply_balances = data["data"]["ztokenRawBalances"]
                    raw_debt_balances = data["data"]["userRawDebts"]

                    if len(raw_supply_balances) == 0 and len(raw_debt_balances) == 0:
                        break
                    skip += PAGINATION_SIZE

                    for item in raw_supply_balances:
                        raw_supply = item["raw_balance"]
                        if item["user"] in raw_balance_per_user:
                            raw_balance_per_user[item["user"]]["supply"] = int(
                                raw_supply
                            )
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
                    print(f"Error: {response.status_code}")
                    raise Exception(f"Error: {response.status_code}")

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


async def main():
    a = [get_data(asset) for asset in assets]
    results = await asyncio.gather(*a)
    df = pd.DataFrame(results)
    df.to_csv("output_zklend.csv", index=False)


async def get_data(asset):
    z_token_int = int(asset["z_token"], 16)
    underlying_int = int(asset["underlying"], 16)
    block_height = await client.get_block_number()

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

    return {
        "protocol": "zkLend",
        "date": get_today(),
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
    }


if __name__ == "__main__":
    asyncio.run(main())
