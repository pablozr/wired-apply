from datetime import date, timedelta

from pydantic import BaseModel, Field, model_validator


class PipelineStartRequest(BaseModel):
    force: bool = False
    days_range: int = Field(default=7, ge=1, le=30, alias="daysRange")
    date_from: date | None = Field(default=None, alias="dateFrom")
    date_to: date | None = Field(default=None, alias="dateTo")
    force_rescore: bool = Field(default=False, alias="forceRescore")

    model_config = {"populate_by_name": True}

    @model_validator(mode="after")
    def validate_date_window(self):
        has_date_from = self.date_from is not None
        has_date_to = self.date_to is not None

        if has_date_from != has_date_to:
            raise ValueError("dateFrom and dateTo must be informed together")

        if has_date_from and has_date_to and self.date_from > self.date_to:
            raise ValueError("dateFrom must be less than or equal to dateTo")

        if has_date_from and has_date_to:
            days_span = (self.date_to - self.date_from).days + 1
            if days_span > 30:
                raise ValueError("date range cannot exceed 30 days")

        return self

    def resolve_window(self) -> tuple[date, date]:
        if self.date_from and self.date_to:
            return self.date_from, self.date_to

        current_date = date.today()
        date_from = current_date - timedelta(days=max(1, int(self.days_range)) - 1)
        return date_from, current_date
