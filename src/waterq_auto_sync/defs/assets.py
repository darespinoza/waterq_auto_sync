from .constants import *
from .resources import PostgresResource
from  .tools import coerse_float
from datetime import date, datetime
from sqlalchemy import (
    MetaData,
    Table,
)
from sqlalchemy.dialects.postgresql import insert

import dagster as dg
import pandas as pd
import requests
import time


@dg.asset()
def pg_waterq_stations(context: dg.AssetExecutionContext,
                    postgres_rsc: PostgresResource,) -> pd.DataFrame:
    """
    Queries to Indice_Calidad database to get stations identifiers and names
    """
    
    engine = None
    try:
        # Get SQLAlchemy engine
        engine = postgres_rsc.get_engine()
        
        # Query to Postgres
        sql_query = """SELECT trim(em.codigo) AS cod_estacion, 
                        TRIM(em.estacion) AS estacion
                    FROM public.estaciones_medicion em;"""
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
            
@dg.asset()
def mfqb_data_raw (context: dg.AssetExecutionContext,
                pg_waterq_stations: pd.DataFrame,
                postgres_rsc: PostgresResource,) -> pd.DataFrame:
    
    """
    Requests data from ETAPA swmfbq endpoint, returns a DataFrame with results for all stations.
    Upload request results to etapa_swmfbq table
    """
    
    engine = None
    # Endpoint request results
    df_raw = pd.DataFrame(columns=['timestamp', 'codigo',  'response'])
    
    try:
        # Current timestamp for requests pkey
        now = datetime.now()
        timestamp_string = now.strftime("%Y-%m-01 00:00:00")
        
        # Create requests for all stations, transform to DataFrames and save a CSV result file
        for index, row in pg_waterq_stations.iterrows():
            # Prepare request's header and payload
            headers = {"Content-Type":"application/json"}
            payload = {"estacion": row['cod_estacion']}
            
            # Perform request
            # Expected result keys: parametro, abreviacion, fecha (YYYY), valor
            try:
                context.log.info(f"Requesting swmfbq endpoint for {row['estacion']} ({row['cod_estacion']}) data")
                req = requests.post(URL_MFQB, headers=headers, json=payload)
                
                # Add a new row to DataFrame
                df_raw.loc[len(df_raw)] = [timestamp_string, row['cod_estacion'], req.text] 
            
            except Exception as exc_req:
                context.log.error(f"Error requesting swmfbq endpoint for {row['cod_estacion']} .\n{str(exc)}")
            
            # Wait a bit and after perform next request
            secs = 60
            context.log.info(f"Waiting {secs} seconds for next request")
            time.sleep(secs)

        # Upload each station response result (raw data)
        if len(df_raw) > 0:
            context.log.info(df_raw)
            # Get SQLAlchemy engine
            engine = postgres_rsc.get_engine()
            
            # Reflect etapa_swmfbq_data table
            metadata = MetaData()
            etapa_swmfbq_raw = Table(
                "etapa_swmfbq_raw",
                metadata,
                schema="public",
                autoload_with=engine
            )

            # Convert DataFrame to list of dicts
            records = df_raw.to_dict(orient="records")

            # Build an UPSERT statement
            stmt = insert(etapa_swmfbq_raw).values(records)
            stmt = stmt.on_conflict_do_update(
                index_elements=["timestamp", "codigo"],
                set_= {
                    "response": stmt.excluded.response,
                }
            )

            # Execute statement
            with engine.begin() as conn:
                conn.execute(stmt)

        # Return DataFrame
        return df_raw

    except Exception as exc:
        context.log.error(f"Error Extracting swmfbq data from ETAPA.\n{str(exc)}")
        return df_raw
    finally:
        # Dipose engine
        if engine:
            engine.dispose()
            
