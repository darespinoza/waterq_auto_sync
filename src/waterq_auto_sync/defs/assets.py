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
import json


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
            
@dg.asset(
    group_name="etapa_swmfqb_to_ierse",
)
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
                context.log.info(f"Requesting swmfbq endpoint for {row['cod_estacion']} - {row['estacion']} data")
                req = requests.post(URL_MFQB, headers=headers, json=payload)
                
                # Add a new row to DataFrame
                df_raw.loc[len(df_raw)] = [timestamp_string, row['cod_estacion'], req.text] 
            
            except Exception as exc_req:
                context.log.error(f"Error requesting swmfbq endpoint for {row['cod_estacion']} data.\n{str(exc)}")
            
            # Wait a bit and after perform next request
            secs = 60
            context.log.info(f"Waiting {secs} seconds for next request")
            time.sleep(secs)

        # Upload each station response result (raw data)
        if len(df_raw) > 0:
            # Visualize DataFrame
            context.log.info(df_raw.dtypes)
            context.log.info(df_raw.head())
            
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

@dg.asset(
    group_name="etapa_swmfqb_to_ierse",
)
def mfqb_data_bronze (context: dg.AssetExecutionContext,
                mfqb_data_raw: pd.DataFrame,
                postgres_rsc: PostgresResource,) -> pd.DataFrame:
    """
    Transform raw responses for each station in a structured DataFrame
    Perform data cleaning to check if values-dates exists and coerce numeric values
    """
    
    engine = None
    # Endpoint request results
    df_transf = pd.DataFrame()
    try:
        # Transform each raw response and upload to db
        for index, row in mfqb_data_raw.iterrows():
            
            try:
                # Load response text as JSON
                r_resp = json.loads(row['response'])
                
                # Transform dict to DataFrame
                rows = []
                for p in r_resp["parametros"]:
                    for m in p["mediciones"]:
                        # Check if mediciones exist
                        if m["fecha"] and m["valor"]:
                            rows.append({
                                "parametro": p["nombre"],
                                "abreviacion": p["abreviacion"],
                                # Add -mm-dd hh:mm:ss to create a timestamp
                                "fecha": f"{m["fecha"]}{DATEF_MFQB}" if m["fecha"] else "",
                                # Coerce not numeric values
                                "valor": coerse_float(m["valor"]),   
                            })
                
                # Convert rows to DataFrame
                df = pd.DataFrame(rows)
                
                # Add station code and transform string to datetime
                if len(df) > 0:
                    df['codigo'] = row['codigo']
                    df["fecha"] = pd.to_datetime(df["fecha"])
                    
                # Concat result DataFrame to all stations DataFrame
                df_transf = pd.concat([df_transf, df], ignore_index=True)
            except Exception as exc_req:
                context.log.error(f"Error parsing swmfbq endpoint response for {row['codigo']}.\n{str(exc)}")
        
        # Upload swmfbq bronze data to IERSE database
        if len(df_transf) > 0:
            # Visualize DataFrame
            context.log.info(df_transf.dtypes)
            context.log.info(df_transf.head())
            
            # Get SQLAlchemy engine
            engine = postgres_rsc.get_engine()
            
            # Reflect etapa_swmfbq_data table
            metadata = MetaData()
            etapa_swmfbq_data = Table(
                "etapa_swmfbq_data",
                metadata,
                schema="public",
                autoload_with=engine
            )

            # Convert DataFrame to list of dicts
            records = df_transf.to_dict(orient="records")
            
            # Build an UPSERT statement
            stmt = insert(etapa_swmfbq_data).values(records)
            stmt = stmt.on_conflict_do_update(
                index_elements=["codigo", "parametro", "fecha"],
                set_= {
                    "abreviacion": stmt.excluded.abreviacion,
                    "valor": stmt.excluded.valor,
                }
            )

            # Execute statement
            with engine.begin() as conn:
                conn.execute(stmt)
        
        # Return DataFrame
        return df_transf
    except Exception as exc:
        context.log.error(f"Error Transforming swmfbq data from ETAPA.\n{str(exc)}")
        return df_transf
    finally:
        # Dipose engine
        if engine:
            engine.dispose()
            
@dg.asset(
    group_name="etapa_swmfqb_to_ierse",
)
def mfqb_data_silver (context: dg.AssetExecutionContext,
                mfqb_data_bronze: pd.DataFrame,
                postgres_rsc: PostgresResource,) -> None:
    
    engine = None
    try:
        # Pick BMWP data only
        df_bmwp = mfqb_data_bronze[mfqb_data_bronze['parametro'] == 'BMWP']
        
        # Rename columns to match IERSE database
        df_bmwp.rename(columns={'codigo': 'cod_estacion',
                                'fecha': 'fecha_reg',
                                'valor': 'valorbmwp'}, inplace=True)   
        
        # Add columns to match IERSE database
        df_bmwp['origen'] = 'waterq_auto_sync'
        df_bmwp['habilitado'] = True
        
        # Remove unused columns
        df_bmwp.drop(columns=['parametro', 'abreviacion'], inplace=True)
        
        # Upload BMWP silver data to IERSE database
        if len(df_bmwp) > 0:
            # Visualize DataFrame
            context.log.info(df_bmwp.dtypes)
            context.log.info(df_bmwp.head())
            
            # Get SQLAlchemy engine
            engine = postgres_rsc.get_engine()
            
            # Reflect etapa_swmfbq_data table
            metadata = MetaData()
            registro_bwmp = Table(
                "registro_bmwp",
                metadata,
                schema="public",
                autoload_with=engine
            )

            # Convert DataFrame to list of dicts
            records = df_bmwp.to_dict(orient="records")
            
            # Build an UPSERT statement
            stmt = insert(registro_bwmp).values(records)
            stmt = stmt.on_conflict_do_update(
                index_elements=["cod_estacion", "fecha_reg"],
                set_= {
                    "habilitado": stmt.excluded.habilitado,
                    "origen": stmt.excluded.origen,
                    "valorbmwp": stmt.excluded.valorbmwp,
                }
            )

            # Execute statement
            with engine.begin() as conn:
                conn.execute(stmt)
        
        # Finish asset execution
        pass
        
    except Exception as exc:
        context.log.error(f"Error upLoading BMWP data from ETAPA to IERSE.\n{str(exc)}")
        pass
    finally:
        # Dipose engine
        if engine:
            engine.dispose()