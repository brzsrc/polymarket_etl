import asyncio
import json
import pickle
from pathlib import Path

from polymarket import AsyncPublicClient, Market
import msgspec
from src.common.utilities import DIR_ROOT
from src.market.gamma_client import GammaClient

token_ids = ["61978737479901466625464242078343767295044975327673383203490527451747949395903",
             "13681304116661254431154581096297814137221891146740558923291472925333589831782"]


async def main() -> None:
    ...


async def run_gamma() -> None:
    out = DIR_ROOT / 'data/markets/epl-bre-che-2026-09-18'
    out.mkdir(parents=True, exist_ok=True)

    # async with AsyncPublicClient() as client:
    #     event = await client.get_event(
    #         slug="epl-bre-che-2026-09-18",
    #     )
    #     for market in event.markets:
    #         with open(out / f"{market.id}.pkl", "wb") as f:
    #             pickle.dump(market, f)

    with open(out / "4237404.pkl", "rb") as f:
        market = pickle.load(f)

    print(isinstance(market, Market))
    print(market.model_dump_json(indent=4))


    # market = await client.get_market(
    #     slug="will-the-us-confirm-that-aliens-exist-before-2027-789-924-249",
    # )
    # market: Market



if __name__ == "__main__":
    # asyncio.run(_main())
    asyncio.run(main())
