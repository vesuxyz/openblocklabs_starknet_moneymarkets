import asyncio
from aiohttp import ClientSession, TCPConnector
from pandas import DataFrame
from starknet_py.contract import Contract
from starknet_py.net.full_node_client import FullNodeClient

from collections import OrderedDict


def nested_odict_to_dict(nested_odict: OrderedDict):
    result = dict(nested_odict)
    for key, value in result.items():
        if isinstance(value, OrderedDict):
            result[key] = nested_odict_to_dict(value)
    return result


async def main():
    CONTRACT_ADDRESS = (
        0x00C318445D5A5096E2AD086452D5C97F65A9D28CAFE343345E0FA70DA0841295
    )

    node_url = "https://starknet-mainnet.public.blastapi.io"

    contract = await Contract.from_address(
        address=CONTRACT_ADDRESS, provider=FullNodeClient(node_url=node_url)
    )

    results = await contract.functions["amp_data"].call()

    amp = [nested_odict_to_dict(item) for item in results]

    df = DataFrame(amp)

    print(df)


asyncio.run(main())
