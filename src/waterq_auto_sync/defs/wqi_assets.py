from .constants import (
    URL_MIE,
    DATEF_MIE,
)
from .resources import PostgresResource
from  .tools import coerse_float
from datetime import date, datetime
from sqlalchemy import (
    MetaData,
    Table,
)
from sqlalchemy.dialects.postgresql import insert
from .assets import (
    pg_waterq_stations,
)

import dagster as dg
import pandas as pd
import requests
import time
import json

@dg.asset(
    group_name="etapa_to_ierse_wqi",
)
def mfqagl_data_raw (context: dg.AssetExecutionContext,
                pg_waterq_stations: pd.DataFrame,
                postgres_rsc: PostgresResource,) -> pd.DataFrame:
    
    """
    Requests data from ETAPA swmfqagl endpoint, returns a DataFrame with results for all stations.
    Upload request results to etapa_swmfqagl_raw table using UPSERT operations.
    """
    
    engine = None
    # Endpoint request results DataFrame
    df_raw = pd.DataFrame(columns=['timestamp', 'codigo',  'response'])
    
    try:
        # Current timestamp for requests pkey
        now = datetime.now()
        timestamp_string = now.strftime("%Y-%m-01 00:00:00")
        
        # Create requests for all stations, transform to DataFrames
        for index, row in pg_waterq_stations.iterrows():
            # Prepare request's header and payload
            headers = {"Content-Type":"application/json"}
            payload = {"estacion": row['cod_estacion']}
            
            # Perform request
            # Expected result keys: parametro, abreviacion, fecha (YYYY), valor
            try:
                context.log.info(f"Requesting swmfqagl endpoint for {row['cod_estacion']} - {row['estacion']} data")
                req = requests.post(URL_MIE, headers=headers, json=payload)
                
                # Add a new row to results DataFrame
                df_raw.loc[len(df_raw)] = [timestamp_string, row['cod_estacion'], req.text] 
            
            except Exception as exc_req:
                context.log.error(f"Error requesting swmfqagl endpoint for {row['cod_estacion']} data.\n{str(exc)}")
            
            # Wait a bit beetween requests
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
            
            # Reflect etapa_swmfqagl_raw table
            metadata = MetaData()
            etapa_swmfqagl_raw = Table(
                "etapa_swmfqagl_raw",
                metadata,
                schema="public",
                autoload_with=engine
            )

            # Convert DataFrame to list of dicts
            records = df_raw.to_dict(orient="records")

            # Build an UPSERT statement
            stmt = insert(etapa_swmfqagl_raw).values(records)
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
        context.log.error(f"Error Extracting swmfqagl data from ETAPA.\n{str(exc)}")
        return df_raw
    finally:
        # Dipose engine
        if engine:
            engine.dispose()

@dg.asset(
    group_name="etapa_to_ierse_wqi",
)
def mfqagl_data_bronze (context: dg.AssetExecutionContext,
                mfqagl_data_raw: pd.DataFrame,
                postgres_rsc: PostgresResource,) -> pd.DataFrame:
    """
    Transform raw responses from each station to a structured DataFrame.
    Perform data cleaning to check if values-dates exists and coerce numeric values.
    Uses an UPSERT statement on etapa_swmfqagl_data table.
    """
    
    engine = None
    # Structured DataFrame
    df_transf = pd.DataFrame()
    try:
        # Transform each raw response
        for index, row in mfqagl_data_raw.iterrows():
            
            try:
                # Load response text as JSON
                r_resp = json.loads(row['response'])
                
                # Transform dict to DataFrame rows
                rows = []
                for p in r_resp['parametros']:
                    for m in p['mediciones']:
                        # Check if mediciones exist
                        if m['fecha'] and m['valor']:
                            rows.append({
                                'parametro': p['nombre'],
                                'abreviacion': p['abreviacion'],
                                # Add -mm-dd hh:mm:ss to create a timestamp
                                'fecha': m['fecha'] if m['fecha'] else "",
                                # Coerce not numeric values
                                'valor': coerse_float(m['valor']),   
                            })
                
                # Convert rows to DataFrame
                df = pd.DataFrame(rows)
                
                # Add station code and transform string to datetime
                if len(df) > 0:
                    df['codigo'] = row['codigo']
                    df['fecha'] = pd.to_datetime(df['fecha'], format=DATEF_MIE)
                    
                # Concat result DataFrame to all stations DataFrame
                df_transf = pd.concat([df_transf, df], ignore_index=True)
            except Exception as exc_t:
                context.log.error(f"Error parsing swmfqagl endpoint response for {row['codigo']}.\n{str(exc_t)}")
        
        # Upload swmfqagl bronze data to IERSE database
        if len(df_transf) > 0:
            # Visualize DataFrame
            context.log.info(df_transf.dtypes)
            context.log.info(df_transf.head())
            
            # Get SQLAlchemy engine
            engine = postgres_rsc.get_engine()
            
            # Reflect etapa_swmfqagl_data table
            metadata = MetaData()
            etapa_swmfqagl_data = Table(
                "etapa_swmfqagl_data",
                metadata,
                schema="public",
                autoload_with=engine
            )

            # Convert DataFrame to list of dicts
            records = df_transf.to_dict(orient="records")
            
            # Build an UPSERT statement
            stmt = insert(etapa_swmfqagl_data).values(records)
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
        context.log.error(f"Error Transforming swmfqagl data from ETAPA.\n{str(exc)}")
        return df_transf
    finally:
        # Dipose engine
        if engine:
            engine.dispose()
            
@dg.asset(
    group_name="etapa_to_ierse_wqi",
)
def mfqagl_data_silver (context: dg.AssetExecutionContext,
                mfqagl_data_bronze: pd.DataFrame,
                postgres_rsc: PostgresResource,) -> None:
    """
    Use WQI parametro data to create a DataFrame that matches IERSE registro_wqi database table.
    Add required columns and drop not used ones.
    Perform an UPSERT operations over registro_wqi table.
    """
    
    
    engine = None
    try:
        # Pick wqi data only
        df_wqi = mfqagl_data_bronze[mfqagl_data_bronze['parametro'] == 'WQI']
        
        # Rename columns to match IERSE database table
        df_wqi.rename(columns={'codigo': 'cod_estacion',
                                'fecha': 'fecha_reg',
                                'valor': 'valorwqi'}, inplace=True)   
        
        # Add columns to match IERSE database table
        df_wqi['origen'] = 'waterq_auto_sync'
        df_wqi['habilitado'] = True
        
        # Remove unused columns
        df_wqi.drop(columns=['parametro', 'abreviacion'], inplace=True)
        
        # Upload wqi silver data to IERSE database
        if len(df_wqi) > 0:
            # Visualize DataFrame
            context.log.info(df_wqi.dtypes)
            context.log.info(df_wqi.head())
            
            # Get SQLAlchemy engine
            engine = postgres_rsc.get_engine()
            
            # Reflect etapa_swmfqagl_data table
            metadata = MetaData()
            registro_wqi = Table(
                "registro_wqi",
                metadata,
                schema="public",
                autoload_with=engine
            )

            # Convert DataFrame to list of dicts
            records = df_wqi.to_dict(orient="records")
            
            # Build an UPSERT statement
            stmt = insert(registro_wqi).values(records)
            stmt = stmt.on_conflict_do_update(
                index_elements=["cod_estacion", "fecha_reg"],
                set_= {
                    "habilitado": stmt.excluded.habilitado,
                    "origen": stmt.excluded.origen,
                    "valorwqi": stmt.excluded.valorwqi,
                }
            )

            # Execute statement
            with engine.begin() as conn:
                conn.execute(stmt)
        
        # Finish asset execution
        pass
        
    except Exception as exc:
        context.log.error(f"Error upLoading WQI data from ETAPA to IERSE.\n{str(exc)}")
        pass
    finally:
        # Dipose engine
        if engine:
            engine.dispose()