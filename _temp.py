import os
from flowfile_core.ai.providers.registry import provider_factory
from flowfile_core.ai.providers import Message
import asyncio

async def main() -> None:
    p = provider_factory(
        name="google",
        surface="cmd_k",
        api_key="",
    )
    print("model resolved:", p.default_model, "supports_tools:", p.supports_tools)

    resp = await p.chat(
        messages=[Message(role="user", content="say hello in 5 words")]
    )
    print(resp)


if __name__ == "__main__":
    asyncio.run(main())
