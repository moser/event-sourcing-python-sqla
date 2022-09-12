from typing import Optional
import pydantic


class _Settings(pydantic.BaseSettings):
    db_dsn: str = "postgresql://postgres:docker@localhost:45432/eventsourcing"
    db_echo_sql: bool = False

    class Config:
        env_prefix = "bank_accounts_"

    def get_db_dsn(self, db_name: Optional[str] = None) -> str:
        from urllib.parse import urlparse, urlunparse

        uri = self.db_dsn
        if db_name is not None:
            parts = list(urlparse(uri))
            parts[2] = f"/{db_name}"
            uri = urlunparse(parts)

        return uri

    def get_sqla_engine_params(self, db_name: Optional[str] = None):
        return dict(
            url=self.get_db_dsn(db_name),
            echo=self.db_echo_sql,
            pool_size=3,
            pool_pre_ping=True,
        )


def get() -> _Settings:
    return _Settings()
