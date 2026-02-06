import os
import dagster as dg

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from dotenv import load_dotenv

# Load env vars
load_dotenv()

# Customized ConfigurableResource for Postgres resource
class PostgresResource(dg.ConfigurableResource):
    hostname: str
    port: int = 5432
    database: str
    username: str
    password: str
    
    def get_engine(self) -> Engine:
        connection_uri = f"postgresql://{self.username}:{self.password}@{self.hostname}:{self.port}/{self.database}"
        return create_engine(connection_uri)
    
@dg.definitions
def resources() -> dg.Definitions:
    return dg.Definitions(
        resources={
            "postgres_rsc": PostgresResource(
                hostname=os.getenv("PG_HOST"),
                port=int(os.getenv("PG_PORT")),
                database=os.getenv("PG_DATABASE"),
                username=os.getenv("PG_USER"),
                password=os.getenv("PG_PASSWORD")
            ),
        }
    )