from multiprocessing import cpu_count
import os
from typing import Any, List, Tuple, Type
import base64

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
        if field_name == "DRIVE_WHITELIST" or field_name == "DRIVE_BLACKLIST":
            if value:
                return [x for x in value.split(",")]
            else:
                return []
        return value if value else field.default


class Settings(BaseSettings):
    MAX_DOWNLOAD_THREADS: int = Field(20, env="MAX_DOWNLOAD_THREADS")
    MAX_DRIVE_PROCESSES: int = Field(4, env="MAX_DRIVE_PROCESSES")
    JIT_S3_UPLOAD: bool = Field(False, env="JIT_S3_UPLOAD")
    COMPRESS_DRIVES: bool = Field(False, env="COMPRESS_DRIVES")
    COMPRESSION_ALGORITHM: str = Field("pigz", env="COMPRESSION_ALGORITHM")
    COMPRESSION_PROCESSES: int = Field(cpu_count(), env="COMPRESSION_PROCESSES")
    DRIVE_WHITELIST: List[str] = Field([], env="DRIVE_WHITELIST")
    DRIVE_BLACKLIST: List[str] = Field([], env="DRIVE_BLACKLIST")
    SERVICE_ACCOUNT_FILE: str = Field(
        "service-account-key.json", env="SERVICE_ACCOUNT_FILE"
    )
    SERVICE_ACCOUNT_JSON: str = Field(None, env="SERVICE_ACCOUNT_JSON")
    DELEGATED_ADMIN_EMAIL: EmailStr = Field(None, env="DELEGATED_ADMIN_EMAIL")
    WORKSPACE_CUSTOMER_ID: str = Field(None, env="WORKSPACE_CUSTOMER_ID")
    S3_BUCKET_NAME: str = Field(None, env="S3_BUCKET_NAME")
    S3_ROLE_BASED_ACCESS: bool = Field(False, env="S3_ROLE_BASED_ACCESS")
    S3_ACCESS_KEY: str | None = Field(None, env="S3_ACCESS_KEY")
    S3_SECRET_KEY: str | None = Field(None, env="S3_SECRET_KEY")
    AUTO_CLEANUP: bool = Field(False, env="AUTO_CLEANUP")
    INCLUDE_SHARED_WITH_ME: bool = Field(True, env="INCLUDE_SHARED_WITH_ME")

    @field_validator(
        "MAX_DOWNLOAD_THREADS", "MAX_DRIVE_PROCESSES", "COMPRESSION_PROCESSES"
    )
    def validate_positive_values(cls, v, info):
        if v <= 0:
            raise ValueError(f"{info.field_name} must be positive")
        return v

    @field_validator("COMPRESS_DRIVES")
    def validate_compress_drives(cls, v, info):
        if v and info.data.get("JIT_S3_UPLOAD"):
            raise ValueError("COMPRESS_DRIVES must be False when JIT_S3_UPLOAD is True")
        return v

    @field_validator("COMPRESSION_ALGORITHM")
    def validate_compression_algorithm(cls, v, info):
        if v not in ["pigz", "lz4", "pzstd"]:
            raise ValueError(f"{info.field_name} must be 'pigz' or 'lz4'")
        return v

    @field_validator("SERVICE_ACCOUNT_FILE")
    def validate_file_exists(cls, v, info):
        if not os.path.exists(v):
            # Get the current data being validated
            json_content = os.environ.get("SERVICE_ACCOUNT_JSON")
            if json_content:
                try:
                    decoded_content = base64.b64decode(json_content).decode("utf-8")
                    with open(v, "w") as f:
                        f.write(decoded_content)
                except Exception as e:
                    raise ValueError(
                        f"Failed to create {v} from SERVICE_ACCOUNT_JSON: {str(e)}"
                    )
            else:
                raise ValueError(f"{info.field_name} does not exist")
        return v

    @field_validator("WORKSPACE_CUSTOMER_ID", "S3_BUCKET_NAME")
    def validate_not_none(cls, v, info):
        if v is None or v == "":
            raise ValueError(f"{info.field_name} must be set")
        return v

    @field_validator("S3_ACCESS_KEY", "S3_SECRET_KEY")
    def validate_s3_credentials(cls, v, info):
        if info.data.get("S3_ROLE_BASED_ACCESS"):
            if v is not None:
                raise ValueError(
                    f"When S3_ROLE_BASED_ACCESS is True, {info.field_name} must not be used."
                )
        else:
            if not v:
                raise ValueError(
                    f"When S3_ROLE_BASED_ACCESS is False, {info.field_name} is required"
                )
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
