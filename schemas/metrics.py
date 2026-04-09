from typing import TypedDict


class PipelineMetricsData(TypedDict):
    jobsCount: int
    applicationsCount: int


class PipelineMetricsResponse(TypedDict):
    status: bool
    message: str
    data: PipelineMetricsData
