from .constants import *
from .resources import PostgresResource
from  .tools import coerse_float

import dagster as dg
import pandas as pd
import requests
import math
import time

@dg.asset
def pg_waterq_stations(context: dg.AssetExecutionContext,
                    postgres_rsc: PostgresResource,) -> pd.DataFrame:
    engine = None
    try:
        # Get SQLAlchemy engine
        engine = postgres_rsc.get_engine()
        
        # Query to Postgres
        sql_query = """select trim(em.codigo) as cod_estacion, 
                        trim(em.estacion) as estacion
                    from public.estaciones_medicion em;"""
        df = pd.read_sql(sql_query, con=engine)
        context.log.info(df)
        return df
    except Exception as exc:
        context.log.error(f"While retrieving IERSE Water Quaility stations.\n{str(exc)}")
        return pd.DataFrame
    finally:
        # Dipose engine
        if engine:
            engine.dispose()
            
@dg.asset
def bwmp_data_req (context: dg.AssetExecutionContext,
                pg_waterq_stations: pd.DataFrame) -> None:
    
    df = pg_waterq_stations.head(2)
    
    for index, row in df.iterrows():
        context.log.info(row)
        time.sleep(60)
