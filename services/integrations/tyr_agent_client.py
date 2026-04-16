import asyncio

from core.config.config import AI_TYR_MODEL
from core.logger.logger import logger

_agents: dict[str, object] = {}
_disabled_agents: set[str] = set()
_lock = asyncio.Lock()


async def get_tyr_agent(kind: str, agent_name: str, prompt: str):
    if kind in _disabled_agents:
        return None

    if kind in _agents:
        return _agents[kind]

    async with _lock:
        if kind in _agents:
            return _agents[kind]

        try:
            from tyr_agent import GPTModel, SimpleAgent

            model = GPTModel(AI_TYR_MODEL)
            agent = SimpleAgent(
                prompt_build=prompt,
                agent_name=agent_name,
                model=model,
                use_storage=False,
                use_history=False,
                use_score=False,
            )
            _agents[kind] = agent
            return agent
        except Exception as error:
            logger.warning("tyr_agent_client_init_failed kind=%s error=%s", kind, error)
            _disabled_agents.add(kind)
            return None
