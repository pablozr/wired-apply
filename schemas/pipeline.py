from pydantic import BaseModel


class PipelineStartRequest(BaseModel):
    force: bool = False
