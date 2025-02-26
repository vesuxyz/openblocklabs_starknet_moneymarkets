import asyncio
import json
import pandas as pd

from datetime import datetime
from typing import Dict, List, Union
from starknet_py.contract import Contract
from starknet_py.net.full_node_client import FullNodeClient
from statistics import median


NODE_URL = "https://free-rpc.nethermind.io/mainnet-juno"
SHRINE = "0x0498edfaf50ca5855666a700c25dd629d577eb9afccdf3b5977aec79aee55ada"
EKUBO_ORACLE_EXTENSION = (
    "0x005e470ff654d834983a46b8f29dfa99963d5044b993cb7b9c92243a69dab38f"
)
PRAGMA = "0x2a85bd616f912537c50a49a4076db02c00b29b2cdc8a197ce92ed1837fa875b"


GATE_ABI = json.load(
    open("./openblocklabs_starknet_moneymarkets/protocols/opus/abis/gate.json")
)
SHRINE_ABI = json.load(
    open("./openblocklabs_starknet_moneymarkets/protocols/opus/abis/shrine.json")
)
EKUBO_ORACLE_EXTENSION_ABI = json.load(
    open(
        "./openblocklabs_starknet_moneymarkets/protocols/opus/abis/ekubo_oracle_extension.json"
    )
)
PRAGMA_ABI = json.load(
    open("./openblocklabs_starknet_moneymarkets/protocols/opus/abis/pragma.json")
)

DAI_ADDR = "0x05574eb6b8789a91466f902c380d978e472db68170ff82a5b650b95a58ddf4ad"
USDC_ADDR = "0x053c91253bc9682c04929ca02ed00b3e423f6710d2ee7e0d5ebb06f3ecf368a8"
USDT_ADDR = "0x068f5c6a61780768455de69077e07e89787839bf8166decfbf92b645209c0fb8"
QUOTE_ADDRS = [DAI_ADDR, USDC_ADDR, USDT_ADDR]
QUOTE_DECIMALS = [18, 6, 6]
QUOTE_PRAGMA_PAIR_IDS = [19212080998863684, 6148332971638477636, 6148333044652921668]
TWAP_DURATION = 24 * 60 * 60

COLLATERAL: List[Dict[str, Union[str, int]]] = [
    {
        "asset": "0x49d36570d4e46f48e99674bd3fcc84644ddd6b96f7c741b1562b82f9e004dc7",
        "name": "Ether",
        "symbol": "ETH",
        "decimals": 18,
        "gate": "0x0315ce9c5d3e5772481181441369d8eea74303b9710a6c72e3fcbbdb83c0dab1",
    },
    {
        "asset": "0x04718f5a0fc34cc1af16a1cdee98ffb20c31f5cd61d6ab07201858f4287c938d",
        "name": "Starknet Token",
        "symbol": "STRK",
        "decimals": 18,
        "gate": "0x031a96fe18fe3fdab28822c82c81471f1802800723c8f3e209f1d9da53bc637d",
    },
    {
        "asset": "0x0057912720381af14b0e5c87aa4718ed5e527eab60b3801ebf702ab09139e38b",
        "name": "Starknet Wrapped Staked Ether",
        "symbol": "wstETH",
        "decimals": 18,
        "gate": "0x03dc297a3788751d6d02acfea1b5dcc21a0eee1d34317a91aea2fbd49113ea58",
    },
    {
        "asset": "0x03fe2b97c1fd336e750087d68b9b867997fd64a2661ff3ca5a7c771641e8e7ac",
        "name": "Wrapped BTC",
        "symbol": "WBTC",
        "decimals": 18,
        "gate": "0x05bc1c8a78667fac3bf9617903dbf2c1bfe3937e1d37ada3d8b86bf70fb7926e",
    },
    {
        "asset": USDC_ADDR,
        "name": "USD Coin",
        "symbol": "USDC",
        "decimals": 6,
        "gate": "",
    },
    {
        "asset": USDT_ADDR,
        "name": "Tether USD",
        "symbol": "USDT",
        "decimals": 6,
        "gate": "",
    },
    {
        "asset": "0x028d709c875c0ceac3dce7065bec5328186dc89fe254527084d1689910954b0a",
        "name": "Endur xSTRK",
        "symbol": "xSTRK",
        "decimals": 18,
        "gate": "0x04a3e7dffd8e74a706be9abe6474e07fbbcf41e1be71387514c4977d54dbc428",
    },
    {
        "asset": "0x0356f304b154d29d2a8fe22f1cb9107a9b564a733cf6b4cc47fd121ac1af90c9",
        "name": "Staked Starknet Token",
        "symbol": "sSTRK",
        "decimals": 18,
        "gate": "0x03b709f3ab9bc072a195b907fb2c27688723b6e4abb812a8941def819f929bd8",
    },
]


async def get_collateral_info(
    provider: FullNodeClient,
    collateral_info: Dict[str, Union[str, int]],
    block: int,
    date: str,
) -> Dict[str, Union[str, int, float]]:
    gate = collateral_info["gate"]
    if gate:
        gate = Contract(provider=provider, abi=GATE_ABI, address=gate, cairo_version=1)
        scale = 10 ** collateral_info["decimals"]
        (deposited,) = await gate.functions["get_total_assets"].call(block_number=block)
        deposited /= scale
    else:
        deposited = 0

    return {
        "protocol": "Opus",
        "date": date,
        "market": collateral_info["asset"],
        "tokenSymbol": collateral_info["symbol"],
        "supply_token": deposited,
        "borrow_token": 0,
        "net_supply_token": deposited,
        "non_recursive_supply_token": deposited,
        "block_height": block,
        "lending_index_rate": 1,
    }


def x128_to_decimal(val: int, decimals: int) -> float:
    assert decimals <= 18
    unscaled = (val / 2**128) ** 2
    if decimals == 18:
        return unscaled

    precision_diff = 18 - decimals
    # scale by twice the difference due to earlier multiplication of sqrt value
    scale = 10 ** (precision_diff * 2)
    return unscaled * scale


async def get_median_cash_price(provider: FullNodeClient, block: int) -> float:
    block_timestamp = (await provider.get_block(block_number=block)).timestamp

    ekubo_oracle_extension = Contract(
        provider=provider,
        abi=EKUBO_ORACLE_EXTENSION_ABI,
        address=EKUBO_ORACLE_EXTENSION,
        cairo_version=1,
    )

    pragma = Contract(
        provider=provider,
        abi=PRAGMA_ABI,
        address=PRAGMA,
        cairo_version=1,
    )

    prices = []
    for token, decimals, pair_id in zip(
        QUOTE_ADDRS, QUOTE_DECIMALS, QUOTE_PRAGMA_PAIR_IDS
    ):
        # get CASH/QUOTE price from Ekubo
        (quote_price_x128,) = await ekubo_oracle_extension.functions[
            "get_price_x128_over_period"
        ].call(
            int(SHRINE, 16),
            int(token, 16),
            block_timestamp - TWAP_DURATION,
            block_timestamp,
            block_number=block
        )
        quote_price = x128_to_decimal(quote_price_x128, decimals)

        # multiply quote price with QUOTE/USD price from Pragma
        (pragma_res,) = await pragma.functions["get_data_median"].call(
            {"SpotEntry": pair_id},
            block_number=block
        )
        pragma_price = pragma_res["price"] / 10 ** pragma_res["decimals"]
        price = quote_price * pragma_price
        prices.append(price)

    median_price = median(prices)
    assert median_price != 0.0
    return median_price


async def get_stables_info(
    provider: FullNodeClient,
    block: int,
    date: str,
) -> Dict[str, Union[str, int, float]]:
    shrine = Contract(
        provider=provider, abi=SHRINE_ABI, address=SHRINE, cairo_version=1
    )
    scale = 10**18
    (health,) = await shrine.functions["get_shrine_health"].call(block_number=block)
    debt = health["debt"]["val"] / scale

    cash_info = {
        "protocol": "Opus",
        "date": date,
        "market": "0x0498edfaf50ca5855666a700c25dd629d577eb9afccdf3b5977aec79aee55ada",
        "tokenSymbol": "CASH",
        "supply_token": 0,
        "borrow_token": debt,
        "net_supply_token": -debt,
        "non_recursive_supply_token": 0,
        "block_height": block,
        "lending_index_rate": 1,
    }

    cash_price = await get_median_cash_price(provider, block)

    stables_info = cash_info.copy()
    stables_info["market"] = "0x0stable"
    stables_info["tokenSymbol"] = "STB"

    debt_usd_value = debt * cash_price
    stables_info["borrow_token"] = debt_usd_value
    stables_info["net_supply_token"] = -debt_usd_value

    return [cash_info, stables_info]


async def main():
    """
    Supply your calculation here according to the Guidelines.
    """
    provider = FullNodeClient(node_url=NODE_URL)
    block = await provider.get_block_number()
    today = datetime.now().strftime("%Y-%m-%d")

    res = [
        await get_collateral_info(provider, collateral_info, block, today)
        for collateral_info in COLLATERAL
    ]
    res += await get_stables_info(provider, block, today)
    df = pd.DataFrame(res)
    print(df)
    return df


if __name__ == "__main__":
    asyncio.run(main())
