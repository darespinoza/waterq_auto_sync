from .resources import PostgresResource

import dagster as dg
import pandas as pd



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
            
