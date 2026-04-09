from pydantic import BaseModel, Field, field_validator


class LoginRequestModel(BaseModel):
    email: str
    password: str

    @field_validator("email")
    @classmethod
    def normalize_email(cls, v: str) -> str:
        return v.strip().lower()


class LoginGoogleRequestModel(BaseModel):
    token: str


class ForgetPasswordRequestModel(BaseModel):
    email: str

    @field_validator("email")
    @classmethod
    def normalize_email(cls, v: str) -> str:
        return v.strip().lower()


class ValidateCodeRequest(BaseModel):
    code: str = Field(min_length=6, max_length=6)


class UpdatePasswordRequest(BaseModel):
    password: str = Field(min_length=6)
