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
    },
    {
        "symbol": "USDT",
        "token_decimals": 6,
        "underlying": "0x068f5c6a61780768455de69077e07e89787839bf8166decfbf92b645209c0fb8",
        "z_token": "0x00811d8da5dc8a2206ea7fd0b28627c2d77280a515126e62baa4d78e22714c4a",
    },
    {
        "symbol": "DAI",
        "token_decimals": 18,
        "underlying": "0x00da114221cb83fa859dbdb4c44beeaa0bb37c7537ad5ae66fe5e0efd20e6eb3",
        "z_token": "0x062fa7afe1ca2992f8d8015385a279f49fad36299754fb1e9866f4f052289376",
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
    ]
    cleaned = list(
        map(
            lambda obj: remove_keys(obj, keys_to_remove),
            [*non_stables_result, *stables_result],
        )
    )
    everything = [*cleaned, all_stables_result]

    df = pd.DataFrame(everything)
    df.to_csv("output_zklend.csv", index=False)


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
    }


def get_all_stables_data(assets, block_height):
    stables_raw_balance_per_user = {}
    stables_supply = 0
    stables_debt = 0

    for asset in assets:
        for user, balances in asset["raw_balance_per_user"].items():
            lending_accumulator = asset["lending_accumulator"]
            debt_accumulator = asset["debt_accumulator"]

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

            if user in stables_raw_balance_per_user:
                stables_raw_balance_per_user[user]["supply"] += scaled_face_value_supply
                stables_raw_balance_per_user[user]["debt"] += scaled_face_value_debt
            else:
                stables_raw_balance_per_user[user] = {
                    "supply": scaled_face_value_supply,
                    "debt": scaled_face_value_debt,
                }

        stables_supply += asset["supply_token"]
        # asset["borrow_token"] is already negative
        stables_debt += asset["borrow_token"]

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


if __name__ == "__main__":
    asyncio.run(main())
