from .constants import *
from .resources import PostgresResource
from  .tools import coerse_float

import dagster as dg
import pandas as pd
import requests
import time
from datetime import date

@dg.asset
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
def mfqb_data_req (context: dg.AssetExecutionContext,
                pg_waterq_stations: pd.DataFrame) -> None:
    
    """
    Requests data from ETAPA swmfbq endpoint, returns a DataFrame with results from all stations
    """
    
    try:
        # All stations DataFrame to store results
        df_data = pd.DataFrame()
        
        # Current date to save CSV
        today = date.today()
        
        # Create requests for all stations, transform to DataFrames and save a CSV result file
        for index, row in pg_waterq_stations.iterrows():
            # Prepare request's header and payload
            headers = {"Content-Type":"application/json"}
            payload = {"estacion": row['cod_estacion']}
            
            # Perform request
            # Expected result keys: parametro, abreviacion, fecha (YYYY), valor
            try:
                context.log.info(f"Requesting BMWP data for {row['estacion']} ({row['cod_estacion']}) station")
                req = requests.post(URL_MFQB, headers=headers, json=payload)
                
                # Convert result to dict
                req_dict = req.json()
                context.log.info(f"JSON result first five key-value pairs:")
                context.log.info(list(req_dict.items())[:5])
                
                # Transform dict to DataFrame
                rows = []
                for p in req_dict["parametros"]:
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
                    df['codigo'] = row['cod_estacion']
                    df["fecha"] = pd.to_datetime(df["fecha"])
                    context.log.info("DataFrame five head elements")
                    context.log.info(df.head(5))
            
            except Exception as exc_req:
                context.log.error(f"Error Extracting BMWP data for {row['cod_estacion']}.\n{str(exc)}")
            
            # Concat result DataFrame to all stations DataFrame
            df_data = pd.concat([df_data, df], ignore_index=True)
            
            # Save as CSV
            df_data.to_csv(f'{today.year}-{today.month}-{today.day}_swmfbq.csv', sep=';', encoding='utf-8', index=False)
            
            # Wait a bit and after perform next request
            secs = 60
            context.log.info(f"Waiting {secs} seconds for next request")
            time.sleep(secs)

        # Terminate asset execution
        pass

    except Exception as exc:
        context.log.error(f"Error on Extract & Transform BMWP data from ETAPA.\n{str(exc)}")
    