from multiprocessing import cpu_count
import os
from typing import Any, List, Tuple, Type

from pydantic import EmailStr, Field, field_validator
from pydantic.fields import FieldInfo

from pydantic_settings import (
    BaseSettings,
    EnvSettingsSource,
    PydanticBaseSettingsSource,
)


class MyCustomSource(EnvSettingsSource):
    def prepare_field_value(
        self, field_name: str, field: FieldInfo, value: Any, value_is_complex: bool
    ) -> Any:
        if field_name == "DRIVE_WHITELIST":
            if value:
                return [x for x in value.split(",")]
            else:
                return []
        return value if value else field.default


class Settings(BaseSettings):
    MAX_DOWNLOAD_THREADS: int = Field(20, env="MAX_DOWNLOAD_THREADS")
    MAX_DRIVE_PROCESSES: int = Field(4, env="MAX_DRIVE_PROCESSES")
    COMPRESS_DRIVES: bool = Field(False, env="COMPRESS_DRIVES")
    COMPRESSION_ALGORITHM: str = Field("pigz", env="COMPRESSION_ALGORITHM")
    COMPRESSION_PROCESSES: int = Field(cpu_count(), env="COMPRESSION_PROCESSES")
    DRIVE_WHITELIST: List[str] = Field([], env="DRIVE_WHITELIST")
    SERVICE_ACCOUNT_FILE: str = Field(
        "service-account-key.json", env="SERVICE_ACCOUNT_FILE"
    )
    DELEGATED_ADMIN_EMAIL: EmailStr = Field(None, env="DELEGATED_ADMIN_EMAIL")
    WORKSPACE_CUSTOMER_ID: str = Field(None, env="WORKSPACE_CUSTOMER_ID")
    S3_BUCKET_NAME: str = Field(None, env="S3_BUCKET_NAME")
    S3_ACCESS_KEY: str = Field(None, env="S3_ACCESS_KEY")
    S3_SECRET_KEY: str = Field(None, env="S3_SECRET_KEY")

    @field_validator(
        "MAX_DOWNLOAD_THREADS", "MAX_DRIVE_PROCESSES", "COMPRESSION_PROCESSES"
    )
    def validate_positive_values(cls, v, info):
        if v <= 0:
            raise ValueError(f"{info.field_name} must be positive")
        return v

    @field_validator("COMPRESSION_ALGORITHM")
    def validate_compression_algorithm(cls, v, info):
        if v not in ["pigz", "lz4"]:
            raise ValueError(f"{info.field_name} must be 'pigz' or 'lz4'")
        return v

    @field_validator("SERVICE_ACCOUNT_FILE")
    def validate_file_exists(cls, v, info):
        if not os.path.exists(v):
            raise ValueError(f"{info.field_name} does not exist")
        return v

    @field_validator(
        "WORKSPACE_CUSTOMER_ID", "S3_BUCKET_NAME", "S3_ACCESS_KEY", "S3_SECRET_KEY"
    )
    def validate_not_none(cls, v, info):
        if v is None or v == "":
            raise ValueError(f"{info.field_name} must be set")
        return v

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: Type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> Tuple[PydanticBaseSettingsSource, ...]:
        return (MyCustomSource(settings_cls),)
