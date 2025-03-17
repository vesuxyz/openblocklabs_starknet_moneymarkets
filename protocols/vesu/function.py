import asyncio
from starknet_py.contract import Contract
from starknet_py.net.full_node_client import FullNodeClient
from datetime import datetime
import pandas as pd
from itertools import permutations

# Configs
# In addition to the configs below, make sure to add example.env vars
NODE_URL = "https://starknet-mainnet.public.blastapi.io"
SINGLETON=0x2545b2e5d519fc230e9cd781046d3a64e092114f07e44771e0d719d148725ef
ELIGIBLE = ["STRK", "ETH", "USDC", "USDT", "xSTRK", "wstETH", "WBTC", "CASH"]
STABLES = ["USDC", "USDT"]
SCALE = 10**18
MARKETS = [
    { # Genesis pool
        "pool": "0x4dc4f0ca6ea4961e4c8373265bfd5317678f4fe374d76f3fd7135f57763bf28",
        "extension": "0x2334189e831d804d4a11d3f71d4a982ec82614ac12ed2e9ca2f8da4e6374fa",
        "asset": "0x049d36570d4e46f48e99674bd3fcc84644ddd6b96f7c741b1562b82f9e004dc7",
        "name": "Ether",
        "symbol": "ETH",
        "decimals": 18,
        "vToken": "0x021fe2ca1b7e731e4a5ef7df2881356070c5d72db4b2d19f9195f6b641f75df0"
    }, 
    {
        "pool": "0x4dc4f0ca6ea4961e4c8373265bfd5317678f4fe374d76f3fd7135f57763bf28",
        "extension": "0x2334189e831d804d4a11d3f71d4a982ec82614ac12ed2e9ca2f8da4e6374fa",
        "asset": "0x03fe2b97c1fd336e750087d68b9b867997fd64a2661ff3ca5a7c771641e8e7ac",
        "name": "Wrapped BTC",
        "symbol": "WBTC",
        "decimals": 8,
        "vToken": "0x06b0ef784eb49c85f4d9447f30d7f7212be65ce1e553c18d516c87131e81dbd6"
    },
    {
        "pool": "0x4dc4f0ca6ea4961e4c8373265bfd5317678f4fe374d76f3fd7135f57763bf28",
        "extension": "0x2334189e831d804d4a11d3f71d4a982ec82614ac12ed2e9ca2f8da4e6374fa",
        "asset": "0x053c91253bc9682c04929ca02ed00b3e423f6710d2ee7e0d5ebb06f3ecf368a8",
        "name": "USD Coin",
        "symbol": "USDC",
        "decimals": 6,
        "vToken": "0x01610abab2ff987cdfb5e73cccbf7069cbb1a02bbfa5ee31d97cc30e29d89090"
    },
    {
        "pool": "0x4dc4f0ca6ea4961e4c8373265bfd5317678f4fe374d76f3fd7135f57763bf28",
        "extension": "0x2334189e831d804d4a11d3f71d4a982ec82614ac12ed2e9ca2f8da4e6374fa",
        "asset": "0x068f5c6a61780768455de69077e07e89787839bf8166decfbf92b645209c0fb8",
        "name": "Tether USD",
        "symbol": "USDT",
        "decimals": 6,
        "vToken": "0x032dd20efeb027ee51e676280df60c609ac6f6dcff798e4523515bc1668ed715"
    },
    {
        "pool": "0x4dc4f0ca6ea4961e4c8373265bfd5317678f4fe374d76f3fd7135f57763bf28",
        "extension": "0x2334189e831d804d4a11d3f71d4a982ec82614ac12ed2e9ca2f8da4e6374fa",
        "asset": "0x57912720381af14b0e5c87aa4718ed5e527eab60b3801ebf702ab09139e38b",
        "name": "Wrapped Staked Ether",
        "symbol": "wstETH",
        "decimals": 18,
        "vToken": "0x0159133f57b09260c707ff9d3ce93abdedee740795cc64a9233a0d61f15af923"
    },
    {
        "pool": "0x4dc4f0ca6ea4961e4c8373265bfd5317678f4fe374d76f3fd7135f57763bf28",
        "extension": "0x2334189e831d804d4a11d3f71d4a982ec82614ac12ed2e9ca2f8da4e6374fa",
        "asset": "0x04718f5a0fc34cc1af16a1cdee98ffb20c31f5cd61d6ab07201858f4287c938d",
        "name": "Starknet Token",
        "symbol": "STRK",
        "decimals": 18,
        "vToken": "0x037ae3f583c8d644b7556c93a04b83b52fa96159b2b0cbd83c14d3122aef80a2"
    },
    { # Re7 xSTRK pool
        "pool": "0x052fb52363939c3aa848f8f4ac28f0a51379f8d1b971d8444de25fbd77d8f161",
        "extension": "0x07cf3881eb4a58e76b41a792fa151510e7057037d80eda334682bd3e73389ec0",
        "asset": "0x028d709c875c0ceac3dce7065bec5328186dc89fe254527084d1689910954b0a",
        "name": "Endur xSTRK",
        "symbol": "xSTRK",
        "decimals": 18,
        "vToken": "0x037ff012710c5175004687bc4d9e4c6e86d6ce5ca6fb6afee72ea02b1208fdb7"
    },
    {
        "pool": "0x052fb52363939c3aa848f8f4ac28f0a51379f8d1b971d8444de25fbd77d8f161",
        "extension": "0x07cf3881eb4a58e76b41a792fa151510e7057037d80eda334682bd3e73389ec0",
        "asset": "0x04718f5a0Fc34cC1AF16A1cdee98fFB20C31f5cD61D6Ab07201858f4287c938D",
        "name": "Starknet Token",
        "symbol": "STRK",
        "decimals": 18,
        "vToken": "0x01f876e2da54266911d8a7409cba487414d318a2b6540149520bf7e2af56b93c"
    },
    { # Re7 sSTRK pool
        "pool": "0x02e06b705191dbe90a3fbaad18bb005587548048b725116bff3104ca501673c1",
        "extension": "0x07cf3881eb4a58e76b41a792fa151510e7057037d80eda334682bd3e73389ec0",
        "asset": "0x0356f304b154d29d2a8fe22f1cb9107a9b564a733cf6b4cc47fd121ac1af90c9",
        "name": "Staked Starknet Token",
        "symbol": "sSTRK",
        "decimals": 18,
        "vToken": "0x047b7947c36be505131384540cb52e869db715b359b98099ade4015bfc8be341"
    },
    {
        "pool": "0x02e06b705191dbe90a3fbaad18bb005587548048b725116bff3104ca501673c1",
        "extension": "0x07cf3881eb4a58e76b41a792fa151510e7057037d80eda334682bd3e73389ec0",
        "asset": "0x04718f5a0fc34cc1af16a1cdee98ffb20c31f5cd61d6ab07201858f4287c938d",
        "name": "Starknet Token",
        "symbol": "STRK",
        "decimals": 18,
        "vToken": "0x05afdf4d18501d1d9d4664390df8c0786a6db8f28e66caa8800f1c2f51396492"
    },
    { # Re7 USDC pool
        "pool": "0x07f135b4df21183991e9ff88380c2686dd8634fd4b09bb2b5b14415ac006fe1d",
        "extension": "0x07cf3881eb4a58e76b41a792fa151510e7057037d80eda334682bd3e73389ec0",
        "asset": "0x049d36570d4e46f48e99674bd3fcc84644ddd6b96f7c741b1562b82f9e004dc7",
        "name": "Ether",
        "symbol": "ETH",
        "decimals": 18,
        "vToken": "0x00989d6902edd719fad6f0331122d084d0fc7cf03e7a254eee549fd9ad8e32e3"
    }, 
    {
        "pool": "0x07f135b4df21183991e9ff88380c2686dd8634fd4b09bb2b5b14415ac006fe1d",
        "extension": "0x07cf3881eb4a58e76b41a792fa151510e7057037d80eda334682bd3e73389ec0",
        "asset": "0x03fe2b97c1fd336e750087d68b9b867997fd64a2661ff3ca5a7c771641e8e7ac",
        "name": "Wrapped BTC",
        "symbol": "WBTC",
        "decimals": 8,
        "vToken": "0x03c48b0d8bc4913d7d789fb2717572dcd71bb599f2c6e81beeb24d865c8baf14"
    },
    {
        "pool": "0x07f135b4df21183991e9ff88380c2686dd8634fd4b09bb2b5b14415ac006fe1d",
        "extension": "0x07cf3881eb4a58e76b41a792fa151510e7057037d80eda334682bd3e73389ec0",
        "asset": "0x053c91253bc9682c04929ca02ed00b3e423f6710d2ee7e0d5ebb06f3ecf368a8",
        "name": "USD Coin",
        "symbol": "USDC",
        "decimals": 6,
        "vToken": "0x028795e04b2abaf61266faa81cc02d4d1a6ef8574fef383cdf6185ca580648aa"
    },
    {
        "pool": "0x07f135b4df21183991e9ff88380c2686dd8634fd4b09bb2b5b14415ac006fe1d",
        "extension": "0x07cf3881eb4a58e76b41a792fa151510e7057037d80eda334682bd3e73389ec0",
        "asset": "0x57912720381af14b0e5c87aa4718ed5e527eab60b3801ebf702ab09139e38b",
        "name": "Wrapped Staked Ether",
        "symbol": "wstETH",
        "decimals": 18,
        "vToken": "0x0341c0667fafa526400d29980aec17928732fdd585fd778d58a1f52836708b0e"
    },
    {
        "pool": "0x07f135b4df21183991e9ff88380c2686dd8634fd4b09bb2b5b14415ac006fe1d",
        "extension": "0x07cf3881eb4a58e76b41a792fa151510e7057037d80eda334682bd3e73389ec0",
        "asset": "0x04718f5a0fc34cc1af16a1cdee98ffb20c31f5cd61d6ab07201858f4287c938d",
        "name": "Starknet Token",
        "symbol": "STRK",
        "decimals": 18,
        "vToken": "0x00b5581d0bc94bc984cf79017d0f4b079c7e926af3d79bd92ff66fb451b340df"
    },
    { # Re7 wstETH pool
        "pool": "0x59ae5a41c9ae05eae8d136ad3d7dc48e5a0947c10942b00091aeb7f42efabb7",
        "extension": "0x07cf3881eb4a58e76b41a792fa151510e7057037d80eda334682bd3e73389ec0",
        "asset": "0x049d36570d4e46f48e99674bd3fcc84644ddd6b96f7c741b1562b82f9e004dc7",
        "name": "Ether",
        "symbol": "ETH",
        "decimals": 18,
        "vToken": "0x07bf0e1b5aabaa681cd3dba25c4e0db42fb4f0f564eb275949628435d337fae1"
    }, 
    {
        "pool": "0x59ae5a41c9ae05eae8d136ad3d7dc48e5a0947c10942b00091aeb7f42efabb7",
        "extension": "0x07cf3881eb4a58e76b41a792fa151510e7057037d80eda334682bd3e73389ec0",
        "asset": "0x57912720381af14b0e5c87aa4718ed5e527eab60b3801ebf702ab09139e38b",
        "name": "Wrapped Staked Ether",
        "symbol": "wstETH",
        "decimals": 18,
        "vToken": "0x06deac5804db38126cf33a245ad2a775cbf74d2978ef24c1112bc1218bf4e22b"
    },
    { # Re7 Starknet Ecosystem
        "pool": "0x6febb313566c48e30614ddab092856a9ab35b80f359868ca69b2649ca5d148d",
        "extension": "0x07cf3881eb4a58e76b41a792fa151510e7057037d80eda334682bd3e73389ec0",
        "asset": "0x053c91253bc9682c04929ca02ed00b3e423f6710d2ee7e0d5ebb06f3ecf368a8",
        "name": "USD Coin",
        "symbol": "USDC",
        "decimals": 6,
        "vToken": "0x017f19582c61479f2fe0b6606300e975c0a8f439102f43eeecc1d0e9b3d84350"
    },
    {
        "pool": "0x6febb313566c48e30614ddab092856a9ab35b80f359868ca69b2649ca5d148d",
        "extension": "0x07cf3881eb4a58e76b41a792fa151510e7057037d80eda334682bd3e73389ec0",
        "asset": "0x04718f5a0fc34cc1af16a1cdee98ffb20c31f5cd61d6ab07201858f4287c938d",
        "name": "Starknet Token",
        "symbol": "STRK",
        "decimals": 18,
        "vToken": "0x07560a5077d454fe04d174c284fc3988c702ef7070ff2d1e1b81010e6688204e"
    },
    { # Alterscope wstETH pool
        "pool": "0x5c678347b60b99b72f245399ba27900b5fc126af11f6637c04a193d508dda26",
        "extension": "0x07cf3881eb4a58e76b41a792fa151510e7057037d80eda334682bd3e73389ec0",
        "asset": "0x57912720381af14b0e5c87aa4718ed5e527eab60b3801ebf702ab09139e38b",
        "name": "Wrapped Staked Ether",
        "symbol": "wstETH",
        "decimals": 18,
        "vToken": "0x05fcb6c1732396efa054c66909beeb1d3bca94501153c1a6584b48d6e8463f20"
    },
    {
        "pool": "0x5c678347b60b99b72f245399ba27900b5fc126af11f6637c04a193d508dda26",
        "extension": "0x07cf3881eb4a58e76b41a792fa151510e7057037d80eda334682bd3e73389ec0",
        "asset": "0x049d36570d4e46f48e99674bd3fcc84644ddd6b96f7c741b1562b82f9e004dc7",
        "name": "Ether",
        "symbol": "ETH",
        "decimals": 18,
        "vToken": "0x07e89e7f1bc528785a5f14f0126b3cffd39f53db63d8bb60125b29f70f8b37bc"
    }, 
    {
        "pool": "0x5c678347b60b99b72f245399ba27900b5fc126af11f6637c04a193d508dda26",
        "extension": "0x07cf3881eb4a58e76b41a792fa151510e7057037d80eda334682bd3e73389ec0",
        "asset": "0x03fe2b97c1fd336e750087d68b9b867997fd64a2661ff3ca5a7c771641e8e7ac",
        "name": "Wrapped BTC",
        "symbol": "WBTC",
        "decimals": 8,
        "vToken": "0x057f0d0684968135afecdaac60fe9f9ccf8a276d6f5ac17b71212bc3bb4f5c01"
    },
    {
        "pool": "0x5c678347b60b99b72f245399ba27900b5fc126af11f6637c04a193d508dda26",
        "extension": "0x07cf3881eb4a58e76b41a792fa151510e7057037d80eda334682bd3e73389ec0",
        "asset": "0x053c91253bc9682c04929ca02ed00b3e423f6710d2ee7e0d5ebb06f3ecf368a8",
        "name": "USD Coin",
        "symbol": "USDC",
        "decimals": 6,
        "vToken": "0x0383c278dac0a6f2dae49960dfd189af5ded9a42b4a1f7c2b00b8d11ff82ff5d"
    },
    {
        "pool": "0x5c678347b60b99b72f245399ba27900b5fc126af11f6637c04a193d508dda26",
        "extension": "0x07cf3881eb4a58e76b41a792fa151510e7057037d80eda334682bd3e73389ec0",
        "asset": "0x068f5c6a61780768455de69077e07e89787839bf8166decfbf92b645209c0fb8",
        "name": "Tether USD",
        "symbol": "USDT",
        "decimals": 6,
        "vToken": "0x06713e4ce7d33342208551c67eece4bce950df9fa9fe233f39561eac9d0cabae"
    },
    {
        "pool": "0x5c678347b60b99b72f245399ba27900b5fc126af11f6637c04a193d508dda26",
        "extension": "0x07cf3881eb4a58e76b41a792fa151510e7057037d80eda334682bd3e73389ec0",
        "asset": "0x04718f5a0fc34cc1af16a1cdee98ffb20c31f5cd61d6ab07201858f4287c938d",
        "name": "Starknet Token",
        "symbol": "STRK",
        "decimals": 18,
        "vToken": "0x02eed5de96c7bc425c0e3d92ba16957e66484dca2b9ad7a1ea76d37410950bfa"
    },
    { # Alterscope CASH pool
        "pool": "0x7bafdbd2939cc3f3526c587cb0092c0d9a93b07b9ced517873f7f6bf6c65563",
        "extension": "0x07cf3881eb4a58e76b41a792fa151510e7057037d80eda334682bd3e73389ec0",
        "asset": "0x0498EDFaF50CA5855666a700C25Dd629D577EB9aFcCDf3B5977aEC79AEE55ADA",
        "name": "Cash",
        "symbol": "CASH",
        "decimals": 18,
        "vToken": "0x05eddd0067f51f0ce113efbd2ea38e1796a02194c680f5bd9ce2e5c679122267"
    },
    {
        "pool": "0x7bafdbd2939cc3f3526c587cb0092c0d9a93b07b9ced517873f7f6bf6c65563",
        "extension": "0x07cf3881eb4a58e76b41a792fa151510e7057037d80eda334682bd3e73389ec0",
        "asset": "0x049d36570d4e46f48e99674bd3fcc84644ddd6b96f7c741b1562b82f9e004dc7",
        "name": "Ether",
        "symbol": "ETH",
        "decimals": 18,
        "vToken": "0x06e0ed468fc285132aa2bc8bbb99f85cb12c2bcaa54e5238436f9b5c6fb5941a"
    }, 
    {
        "pool": "0x7bafdbd2939cc3f3526c587cb0092c0d9a93b07b9ced517873f7f6bf6c65563",
        "extension": "0x07cf3881eb4a58e76b41a792fa151510e7057037d80eda334682bd3e73389ec0",
        "asset": "0x03fe2b97c1fd336e750087d68b9b867997fd64a2661ff3ca5a7c771641e8e7ac",
        "name": "Wrapped BTC",
        "symbol": "WBTC",
        "decimals": 8,
        "vToken": "0x0279e954817dd6f1082ca2c1010beb6ba84fa602bbd452902e88c466de451b0f"
    },
    {
        "pool": "0x7bafdbd2939cc3f3526c587cb0092c0d9a93b07b9ced517873f7f6bf6c65563",
        "extension": "0x07cf3881eb4a58e76b41a792fa151510e7057037d80eda334682bd3e73389ec0",
        "asset": "0x053c91253bc9682c04929ca02ed00b3e423f6710d2ee7e0d5ebb06f3ecf368a8",
        "name": "USD Coin",
        "symbol": "USDC",
        "decimals": 6,
        "vToken": "0x04d6cb9637b9af77fa7bfe4336777279eb8766ba30652e5186edc681effc0601"
    },
    {
        "pool": "0x7bafdbd2939cc3f3526c587cb0092c0d9a93b07b9ced517873f7f6bf6c65563",
        "extension": "0x07cf3881eb4a58e76b41a792fa151510e7057037d80eda334682bd3e73389ec0",
        "asset": "0x068f5c6a61780768455de69077e07e89787839bf8166decfbf92b645209c0fb8",
        "name": "Tether USD",
        "symbol": "USDT",
        "decimals": 6,
        "vToken": "0x0361412b442c2d9d98909cca413254d4abc11fbcdeda00290088798deed2ac5f"
    },
    {
        "pool": "0x7bafdbd2939cc3f3526c587cb0092c0d9a93b07b9ced517873f7f6bf6c65563",
        "extension": "0x07cf3881eb4a58e76b41a792fa151510e7057037d80eda334682bd3e73389ec0",
        "asset": "0x04718f5a0fc34cc1af16a1cdee98ffb20c31f5cd61d6ab07201858f4287c938d",
        "name": "Starknet Token",
        "symbol": "STRK",
        "decimals": 18,
        "vToken": "0x0470ef9431a58d99884b7426c4294feadcd4d65a983278037018f8c5737f011a"
    },
    { # Alterscope xSTRK pool
        "pool": "0x27f2bb7fb0e232befc5aa865ee27ef82839d5fad3e6ec1de598d0fab438cb56",
        "extension": "0x07cf3881eb4a58e76b41a792fa151510e7057037d80eda334682bd3e73389ec0",
        "asset": "0x028d709c875c0ceac3dce7065bec5328186dc89fe254527084d1689910954b0a",
        "name": "Endur xSTRK",
        "symbol": "xSTRK",
        "decimals": 18,
        "vToken": "0x062b16a3c933bd60eddc9630c3d088f0a1e9dcd510fbbf4ff3fb3b6a3839fd8a"
    },
    {
        "pool": "0x27f2bb7fb0e232befc5aa865ee27ef82839d5fad3e6ec1de598d0fab438cb56",
        "extension": "0x07cf3881eb4a58e76b41a792fa151510e7057037d80eda334682bd3e73389ec0",
        "asset": "0x049d36570d4e46f48e99674bd3fcc84644ddd6b96f7c741b1562b82f9e004dc7",
        "name": "Ether",
        "symbol": "ETH",
        "decimals": 18,
        "vToken": "0x07050e1231cd1ab65447fa7adddd7038f36c0a7b46740d369cd3cfe925818c78"
    }, 
    {
        "pool": "0x27f2bb7fb0e232befc5aa865ee27ef82839d5fad3e6ec1de598d0fab438cb56",
        "extension": "0x07cf3881eb4a58e76b41a792fa151510e7057037d80eda334682bd3e73389ec0",
        "asset": "0x03fe2b97c1fd336e750087d68b9b867997fd64a2661ff3ca5a7c771641e8e7ac",
        "name": "Wrapped BTC",
        "symbol": "WBTC",
        "decimals": 8,
        "vToken": "0x50d54f68cbcaaba424b92666791d9755c52a636e8a709db977ed429f168a0e"
    },
    {
        "pool": "0x27f2bb7fb0e232befc5aa865ee27ef82839d5fad3e6ec1de598d0fab438cb56",
        "extension": "0x07cf3881eb4a58e76b41a792fa151510e7057037d80eda334682bd3e73389ec0",
        "asset": "0x053c91253bc9682c04929ca02ed00b3e423f6710d2ee7e0d5ebb06f3ecf368a8",
        "name": "USD Coin",
        "symbol": "USDC",
        "decimals": 6,
        "vToken": "0x013ff7f2ad3e9ae7c94391b8271d5ab84f0ba7d59e33f781f3202829e41a028b"
    },
    {
        "pool": "0x27f2bb7fb0e232befc5aa865ee27ef82839d5fad3e6ec1de598d0fab438cb56",
        "extension": "0x07cf3881eb4a58e76b41a792fa151510e7057037d80eda334682bd3e73389ec0",
        "asset": "0x068f5c6a61780768455de69077e07e89787839bf8166decfbf92b645209c0fb8",
        "name": "Tether USD",
        "symbol": "USDT",
        "decimals": 6,
        "vToken": "0x02359a0f14fadc38d236f3ef0550240e8059f2d1c3100df708bbd19e9aff48af"
    },
    {
        "pool": "0x27f2bb7fb0e232befc5aa865ee27ef82839d5fad3e6ec1de598d0fab438cb56",
        "extension": "0x07cf3881eb4a58e76b41a792fa151510e7057037d80eda334682bd3e73389ec0",
        "asset": "0x04718f5a0fc34cc1af16a1cdee98ffb20c31f5cd61d6ab07201858f4287c938d",
        "name": "Starknet Token",
        "symbol": "STRK",
        "decimals": 18,
        "vToken": "0x030ab9c447368fb18c9286f852fcf2082e6f64ec0be07fa08e6d63eb90524746"
    },
    { # Alterscope Cornerstone pool
        "pool": "0x2906e07881acceff9e4ae4d9dacbcd4239217e5114001844529176e1f0982ec",
        "extension": "0x07cf3881eb4a58e76b41a792fa151510e7057037d80eda334682bd3e73389ec0",
        "asset": "0x049d36570d4e46f48e99674bd3fcc84644ddd6b96f7c741b1562b82f9e004dc7",
        "name": "Ether",
        "symbol": "ETH",
        "decimals": 18,
        "vToken": "0x07050e1231cd1ab65447fa7adddd7038f36c0a7b46740d369cd3cfe925818c78"
    }, 
    {
        "pool": "0x2906e07881acceff9e4ae4d9dacbcd4239217e5114001844529176e1f0982ec",
        "extension": "0x07cf3881eb4a58e76b41a792fa151510e7057037d80eda334682bd3e73389ec0",
        "asset": "0x03fe2b97c1fd336e750087d68b9b867997fd64a2661ff3ca5a7c771641e8e7ac",
        "name": "Wrapped BTC",
        "symbol": "WBTC",
        "decimals": 8,
        "vToken": "0x50d54f68cbcaaba424b92666791d9755c52a636e8a709db977ed429f168a0e"
    },
    {
        "pool": "0x2906e07881acceff9e4ae4d9dacbcd4239217e5114001844529176e1f0982ec",
        "extension": "0x07cf3881eb4a58e76b41a792fa151510e7057037d80eda334682bd3e73389ec0",
        "asset": "0x053c91253bc9682c04929ca02ed00b3e423f6710d2ee7e0d5ebb06f3ecf368a8",
        "name": "USD Coin",
        "symbol": "USDC",
        "decimals": 6,
        "vToken": "0x013ff7f2ad3e9ae7c94391b8271d5ab84f0ba7d59e33f781f3202829e41a028b"
    },
    {
        "pool": "0x2906e07881acceff9e4ae4d9dacbcd4239217e5114001844529176e1f0982ec",
        "extension": "0x07cf3881eb4a58e76b41a792fa151510e7057037d80eda334682bd3e73389ec0",
        "asset": "0x068f5c6a61780768455de69077e07e89787839bf8166decfbf92b645209c0fb8",
        "name": "Tether USD",
        "symbol": "USDT",
        "decimals": 6,
        "vToken": "0x02359a0f14fadc38d236f3ef0550240e8059f2d1c3100df708bbd19e9aff48af"
    },
    {
        "pool": "0x2906e07881acceff9e4ae4d9dacbcd4239217e5114001844529176e1f0982ec",
        "extension": "0x07cf3881eb4a58e76b41a792fa151510e7057037d80eda334682bd3e73389ec0",
        "asset": "0x04718f5a0fc34cc1af16a1cdee98ffb20c31f5cd61d6ab07201858f4287c938d",
        "name": "Starknet Token",
        "symbol": "STRK",
        "decimals": 18,
        "vToken": "0x030ab9c447368fb18c9286f852fcf2082e6f64ec0be07fa08e6d63eb90524746"
    }
]

# Fetch data for a specific market from Vesu singleton directly
async def get_market_info(market_info, singleton_contract, provider):
    block = await provider.get_block_number()
    now = datetime.now()
    formatted_date = now.strftime("%Y-%m-%d")
    # singleton.asset_config_unsafe returns per-current block market data
    # - reserve: the remaining liquidity in the market
    # - total_nominal_debt: the total outstanding nominal (excluding accrued interest) debt
    # - last_rate_accumulator: the index tracking accrued interest per current block
    #
    # Notes:
    # - the pool (identified by POOL) does not support "recursive" supply/borrowing
    #   so supply = non-recursive-supply by default
    # - rate_accumulator is an interest accrual index that converts to users' total
    #   borrowed by multiplication with total_nominal_debt
    # - we add the raw rate_accumulator to the return values so 'get_stables_info' 
    #   can reuse, then drop the column of the DataFrame at the end again
    asset_config = (await singleton_contract.functions["asset_config_unsafe"].call(
        int(market_info['pool'], base=16), int(market_info['asset'], base=16)))[0][0]
    asset_scale = asset_config['scale']
    reserve = asset_config['reserve'] / asset_scale
    rate_accumulator = asset_config['last_rate_accumulator'] / SCALE
    total_borrowed = rate_accumulator * asset_config['total_nominal_debt']  / SCALE
    total_supplied = reserve + total_borrowed
    # lending_index_rate is fetched from the pool's vTokens directly
    lending_index_rate = (await asyncio.gather(get_index(market_info, provider)))[0]
    return {
        "protocol": "Vesu",
        "date": formatted_date,
        "market": market_info['asset'],
        "pool_id": market_info['pool'],
        "tokenSymbol": market_info['symbol'],
        "supply_token": total_supplied,
        "borrow_token": total_borrowed,
        "net_supply_token": reserve,
        "non_recursive_supply_token": total_supplied,
        "block_height": block,
        "lending_index_rate": lending_index_rate / asset_scale,
        "rate_accumulator": rate_accumulator
    }

# Fetch data for stablecoins combined
# The extension contract tracks total debt and supplied liquidity per "lending pair".
# We thus clean total supply from recursive supply/borrowing by deducting the debt from
# the recursive lending pairs (e.g. USDC/USDT, USDT/USDC, etc.)
async def get_stables_info(markets, results_markets, provider):
    df_markets = pd.DataFrame(results_markets)
    coroutines = [get_pair_info(pair_info, await Contract.from_address(provider=provider, address=pair_info[0]['extension']), pair_info[0]['pool'])
                      for pair_info in permutations(markets, 2) 
                      if pair_info[0]['symbol'] in STABLES
                      and pair_info[1]['symbol'] in STABLES
                      and pair_info[0]['pool'] == pair_info[1]['pool']]
    results_pairs = await asyncio.gather(*coroutines)
    df_pairs = pd.DataFrame(results_pairs).groupby('asset').sum()
    coroutines = [get_price(market_info, await Contract.from_address(provider=provider, address=market_info['extension']), market_info['pool']) 
                  for market_info in markets 
                  if market_info['symbol'] in STABLES]
    results_prices = await asyncio.gather(*coroutines)
    df_prices = pd.DataFrame(results_prices).groupby('asset').mean().reset_index()
    total_supply = 0
    total_borrow = 0
    total_non_recursive_supplied = 0
    for asset in df_prices.asset:
        supply = df_markets.query('market == @asset').supply_token.iloc[0]
        borrow = df_markets.query('market == @asset').borrow_token.iloc[0]
        recursive_borrow = (df_markets.query('market == @asset').rate_accumulator.iloc[0] * 
            df_pairs.query('asset == @asset').recursive_nominal_debt.iloc[0])
        non_recursive_supplied = supply - recursive_borrow
        price = df_prices.query('asset == @asset').price.iloc[0]
        total_supply += price * supply
        total_borrow += price * borrow
        total_non_recursive_supplied += price * max(0, non_recursive_supplied)
    return {
        "protocol": "Vesu",
        "date": df_markets.date[0],
        "market": "0x0stable",
        "pool_id": df_markets.pool_id[0],
        "tokenSymbol": "STB",
        "supply_token": total_supply,
        "borrow_token": total_borrow,
        "net_supply_token": total_supply - total_borrow,
        "non_recursive_supply_token": total_non_recursive_supplied,
        "block_height": df_markets.block_height[0],
        "lending_index_rate": 1.0
    }

# Fetch total supply and debt for a specific lending pair
async def get_pair_info(pair_info, extension_contract, pool):
    collateral_asset = pair_info[0]['asset']
    debt_asset = pair_info[1]['asset']
    pair_info = (await extension_contract.functions['pairs'].call(
        int(pool, base=16), int(collateral_asset, base=16), int(debt_asset, base=16)))[0]
    return {
        "asset": debt_asset,
        "recursive_nominal_debt": pair_info['total_nominal_debt'] / SCALE
    }

# Fetch the oracle price for a specific asset (pulls from Pragma)
async def get_price(market_info, extension_contract, pool):
    asset = market_info['asset']
    asset_price = (await extension_contract.functions['price'].call(
        int(pool, base=16), int(asset, base=16)))[0]['value']
    return {
        "asset": asset,
        "price": asset_price / SCALE
    }

# Fetch the lending index rate (pulls from vToken)
async def get_index(market_info, provider):
    vToken_contract = await Contract.from_address(provider=provider, address=market_info['vToken'])
    index = (await vToken_contract.functions['convert_to_assets'].call(
        int(SCALE)))[0]
    return index

async def main():
    """
    Supply your calculation here according to the Guidelines.
    """
    provider = FullNodeClient(node_url=NODE_URL)
    singleton_contract = await Contract.from_address(provider=provider, address=SINGLETON)
    coroutines = [get_market_info(market_info, singleton_contract, provider)
                    for market_info in MARKETS 
                    if market_info['symbol'] in ELIGIBLE]
    results_markets = await asyncio.gather(*coroutines)
    results_stables = await get_stables_info(MARKETS, results_markets, provider)
    results_markets.append(results_stables)
    df = pd.DataFrame(results_markets)
    df.drop('rate_accumulator', axis=1, inplace=True)
    print(df.to_string())
    return df

if __name__ == "__main__":
    asyncio.run(main())
