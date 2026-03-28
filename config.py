"""
Configuration settings for Attestation Service
"""

import os
from typing import Optional, List
from pydantic_settings import BaseSettings, SettingsConfigDict
from functools import lru_cache

class Settings(BaseSettings):
    """Application settings"""
    
    app_name: str = "Attestation Service"
    app_version: str = "1.0.0"
    environment: str = "dev"
    debug: bool = False
    
    host: str = "0.0.0.0"
    port: int = 8000
    reload: bool = False
    
    log_level: str = "INFO"
    
    cors_origins: List[str] = ["*"]
    
    firestore_project_id: Optional[str] = None
    firestore_db_name: Optional[str] = "(default)"
    
    gcs_bucket_name: str = "attestation-evidence-bucket"
    
    model_config = SettingsConfigDict(
        env_file=".env",
        case_sensitive=False,
        extra="ignore"
    )

class DevSettings(Settings):
    debug: bool = True
    reload: bool = True
    log_level: str = "DEBUG"
    environment: str = "dev"
    
    model_config = SettingsConfigDict(
        env_file=".env.dev",
        case_sensitive=False,
        extra="ignore"
    )

class ProductionSettings(Settings):
    debug: bool = False
    environment: str = "production"
    log_level: str = "WARNING"
    reload: bool = False
    
    model_config = SettingsConfigDict(
        env_file=".env.prod",
        case_sensitive=False,
        extra="ignore"
    )

def get_settings_class():
    env = os.getenv("APP_ENV", "dev").lower()
    settings_map = {
        "dev": DevSettings,
        "local": DevSettings,
        "production": ProductionSettings,
        "prod": ProductionSettings
    }
    return settings_map.get(env, DevSettings)

@lru_cache()
def get_settings() -> Settings:
    settings_class = get_settings_class()
    return settings_class()
