"""
ETL Pipeline para Brazilian E-Commerce Olist Dataset
=====================================================

Este módulo implementa un pipeline ETL robusto y escalable para cargar datos
de comercio electrónico desde archivos CSV hacia PostgreSQL.

Características:
    - Logging estructurado con diferentes niveles
    - Configuración mediante variables de entorno (.env)
    - Validación de datos y métricas de calidad
    - Manejo robusto de errores
    - Type hints para mejor mantenibilidad
    - Separación de responsabilidades

Author: Eberth Rojas Barbaran
Date: 2025-12-31
"""

import logging
import os
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional, Tuple

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, inspect
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

# Cargar variables de entorno desde archivo .env
load_dotenv()


# ============================================================================
# CONFIGURACIÓN Y CONSTANTES
# ============================================================================

@dataclass
class ETLConfig:
    """Configuración centralizada del pipeline ETL."""
    
    # Directorios
    base_dir: Path = Path(__file__).parent.parent
    csv_folder: Path = base_dir / 'data' / 'raw'
    processed_folder: Path = base_dir / 'data' / 'processed'
    log_folder: Path = base_dir / 'logs'
    
    # Base de datos (cargadas desde archivo .env)
    db_user: str = os.getenv('DB_USER', 'postgres')
    db_password: str = os.getenv('DB_PASSWORD')  # Sin valor por defecto por seguridad
    db_host: str = os.getenv('DB_HOST', 'localhost')
    db_port: str = os.getenv('DB_PORT', '5432')
    db_name: str = os.getenv('DB_NAME', 'ecommerce_olist')
    
    def __post_init__(self):
        """Valida que las credenciales requeridas estén configuradas."""
        if not self.db_password:
            raise ValueError(
                "DB_PASSWORD no está configurada. "
                "Crea un archivo .env con las credenciales de la base de datos."
            )
    
    # Parámetros de carga
    chunk_size: int = 1000
    if_exists: str = 'replace'  # 'replace', 'append', 'fail'
    
    @property
    def connection_string(self) -> str:
        """Genera la cadena de conexión a PostgreSQL."""
        return (
            f"postgresql+psycopg2://{self.db_user}:{self.db_password}"
            f"@{self.db_host}:{self.db_port}/{self.db_name}"
        )


# Modo de ejecución del ETL
class ETLMode:
    """Modos de ejecución disponibles."""
    SQL_ONLY = "sql"          # Solo carga a PostgreSQL
    CSV_ONLY = "csv"          # Solo genera CSV procesados
    HYBRID = "hybrid"         # Ambos: SQL + CSV


# Mapeo de archivos CSV a tablas en la base de datos
# Orden basado en dependencias para evitar problemas de integridad referencial
TABLE_FILE_MAPPING: Dict[str, str] = {
    # Tablas base (sin dependencias)
    "customers": "olist_customers_dataset.csv",
    "sellers": "olist_sellers_dataset.csv",
    "products": "olist_products_dataset.csv",
    "product_translation": "product_category_name_translation.csv",
    "geolocation": "olist_geolocation_dataset.csv",
    
    # Tablas dependientes
    "orders": "olist_orders_dataset.csv",
    "items": "olist_order_items_dataset.csv",
    "payments": "olist_order_payments_dataset.csv",
    "reviews": "olist_order_reviews_dataset.csv",
}


# ============================================================================
# CONFIGURACIÓN DE LOGGING
# ============================================================================

def setup_logging(config: ETLConfig) -> logging.Logger:
    """
    Configura el sistema de logging con handlers para consola y archivo.
    
    Args:
        config: Configuración del ETL
        
    Returns:
        Logger configurado
    """
    # Crear directorio de logs si no existe
    config.log_folder.mkdir(parents=True, exist_ok=True)
    
    # Nombre del archivo de log con timestamp
    log_filename = config.log_folder / f"etl_{datetime.now():%Y%m%d_%H%M%S}.log"
    
    # Configuración del logger
    logger = logging.getLogger('ETL_Olist')
    logger.setLevel(logging.DEBUG)
    
    # Formato detallado
    formatter = logging.Formatter(
        '%(asctime)s | %(levelname)-8s | %(funcName)s | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Handler para archivo
    file_handler = logging.FileHandler(log_filename, encoding='utf-8')
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)
    
    # Handler para consola
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)
    
    # Agregar handlers
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    logger.info(f"Log file: {log_filename}")
    
    return logger


# ============================================================================
# FUNCIONES DE BASE DE DATOS
# ============================================================================

def create_database_engine(config: ETLConfig, logger: logging.Logger) -> Optional[Engine]:
    """
    Crea y valida la conexión al motor de base de datos.
    
    Args:
        config: Configuración del ETL
        logger: Logger para registro de eventos
        
    Returns:
        Engine de SQLAlchemy o None si falla la conexión
    """
    try:
        logger.info("Creando conexión a PostgreSQL...")
        logger.debug(f"Host: {config.db_host}, Database: {config.db_name}")
        
        engine = create_engine(
            config.connection_string,
            pool_pre_ping=True,  # Verifica conexiones antes de usarlas
            echo=False  # Cambiar a True para debug SQL
        )
        
        # Validar conexión
        with engine.connect() as conn:
            logger.info("✓ Conexión a base de datos exitosa")
            
            # Mostrar tablas existentes
            inspector = inspect(engine)
            existing_tables = inspector.get_table_names()
            if existing_tables:
                logger.info(f"Tablas existentes: {', '.join(existing_tables)}")
        
        return engine
        
    except SQLAlchemyError as e:
        logger.error(f"✗ Error al conectar a la base de datos: {e}")
        logger.error("Verifica credenciales y que PostgreSQL esté ejecutándose")
        return None
    except Exception as e:
        logger.error(f"✗ Error inesperado: {e}")
        return None


# ============================================================================
# FUNCIONES DE TRANSFORMACIÓN Y VALIDACIÓN
# ============================================================================

def validate_and_transform_dataframe(
    df: pd.DataFrame, 
    table_name: str, 
    logger: logging.Logger
) -> Tuple[pd.DataFrame, Dict[str, any]]:
    """
    Valida y transforma un DataFrame con métricas de calidad.
    
    Args:
        df: DataFrame a validar y transformar
        table_name: Nombre de la tabla
        logger: Logger para registro
        
    Returns:
        Tupla con DataFrame transformado y diccionario de métricas
    """
    logger.debug(f"Validando datos de '{table_name}'...")
    
    # Métricas iniciales
    initial_rows = len(df)
    initial_cols = len(df.columns)
    
    # 1. Normalizar nombres de columnas
    df.columns = df.columns.str.lower().str.strip().str.replace(' ', '_')
    
    # 2. Identificar duplicados
    duplicates = df.duplicated().sum()
    if duplicates > 0:
        logger.warning(f"  ⚠ {duplicates} filas duplicadas encontradas")
    
    # 3. Analizar valores nulos
    null_counts = df.isnull().sum()
    null_cols = null_counts[null_counts > 0]
    if not null_cols.empty:
        logger.debug(f"  Columnas con nulos: {dict(null_cols)}")
    
    # 4. Detectar y manejar tipos de datos
    date_columns = [col for col in df.columns if 'date' in col or 'time' in col]
    for col in date_columns:
        try:
            df[col] = pd.to_datetime(df[col], errors='coerce')
            logger.debug(f"  Columna '{col}' convertida a datetime")
        except Exception:
            logger.warning(f"  No se pudo convertir '{col}' a datetime")
    
    # Métricas de calidad
    metrics = {
        'initial_rows': initial_rows,
        'final_rows': len(df),
        'columns': initial_cols,
        'duplicates': duplicates,
        'total_nulls': df.isnull().sum().sum(),
        'null_percentage': round(df.isnull().sum().sum() / (initial_rows * initial_cols) * 100, 2),
        'memory_usage_mb': round(df.memory_usage(deep=True).sum() / 1024**2, 2)
    }
    
    return df, metrics


# ============================================================================
# FUNCIÓN PRINCIPAL DE CARGA
# ============================================================================

def load_table_to_database(
    table_name: str,
    file_path: Path,
    engine: Engine,
    config: ETLConfig,
    logger: logging.Logger
) -> bool:
    """
    Carga un archivo CSV individual a la base de datos.
    
    Args:
        table_name: Nombre de la tabla destino
        file_path: Ruta al archivo CSV
        engine: Engine de SQLAlchemy
        config: Configuración del ETL
        logger: Logger
        
    Returns:
        True si la carga fue exitosa, False en caso contrario
    """
    try:
        logger.info(f"📊 Procesando tabla: '{table_name}'")
        logger.debug(f"  Archivo: {file_path.name}")
        
        # EXTRACT: Leer CSV
        logger.debug("  [1/3] Extrayendo datos...")
        df = pd.read_csv(file_path, low_memory=False)
        logger.debug(f"  ✓ {len(df)} filas leídas")
        
        # TRANSFORM: Validar y transformar
        logger.debug("  [2/3] Transformando datos...")
        df, metrics = validate_and_transform_dataframe(df, table_name, logger)
        
        # LOAD: Cargar a SQL
        logger.debug("  [3/3] Cargando a base de datos...")
        df.to_sql(
            name=table_name,
            con=engine,
            if_exists=config.if_exists,
            index=False,
            chunksize=config.chunk_size,
            method='multi'  # Inserción por lotes más rápida
        )
        
        # Reportar éxito con métricas
        logger.info(
            f"  ✓ Carga exitosa | "
            f"Filas: {metrics['final_rows']:,} | "
            f"Columnas: {metrics['columns']} | "
            f"Nulos: {metrics['null_percentage']}% | "
            f"Memoria: {metrics['memory_usage_mb']} MB"
        )
        
        return True
        
    except FileNotFoundError:
        logger.error(f"  ✗ Archivo no encontrado: {file_path}")
        return False
    except pd.errors.EmptyDataError:
        logger.error(f"  ✗ Archivo vacío: {file_path}")
        return False
    except SQLAlchemyError as e:
        logger.error(f"  ✗ Error de base de datos: {e}")
        return False
    except Exception as e:
        logger.error(f"  ✗ Error inesperado: {e}", exc_info=True)
        return False


# ============================================================================
# FUNCIONES DE PROCESAMIENTO STAR SCHEMA (DIRECTO DESDE CSV)
# ============================================================================

def load_raw_dataframes(config: ETLConfig, logger: logging.Logger) -> Dict[str, pd.DataFrame]:
    """
    Carga todos los archivos CSV raw en memoria.
    
    Args:
        config: Configuración del ETL
        logger: Logger
        
    Returns:
        Diccionario con DataFrames cargados
    """
    logger.debug("Cargando archivos CSV raw en memoria...")
    dfs = {}
    
    for table_name, file_name in TABLE_FILE_MAPPING.items():
        file_path = config.csv_folder / file_name
        if file_path.exists():
            df = pd.read_csv(file_path, low_memory=False)
            # Normalizar nombres de columnas
            df.columns = df.columns.str.lower().str.strip().str.replace(' ', '_')
            dfs[table_name] = df
            logger.debug(f"  ✓ {table_name}: {len(df):,} filas")
    
    return dfs


def create_dim_customers(dfs: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Crea dimensión de clientes."""
    return dfs['customers'][[
        'customer_id', 'customer_unique_id', 'customer_zip_code_prefix',
        'customer_city', 'customer_state'
    ]].copy()


def create_dim_sellers(dfs: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Crea dimensión de vendedores."""
    return dfs['sellers'][[
        'seller_id', 'seller_zip_code_prefix', 'seller_city', 'seller_state'
    ]].copy()


def create_dim_products(dfs: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Crea dimensión de productos con traducción."""
    products = dfs['products'].copy()
    translation = dfs['product_translation'].copy()
    
    # Merge con traducción
    dim = products.merge(
        translation,
        on='product_category_name',
        how='left'
    )
    
    # Rellenar categorías sin traducción
    dim['product_category_name_english'] = dim['product_category_name_english'].fillna(
        dim['product_category_name']
    )
    
    return dim[[
        'product_id', 'product_category_name', 'product_category_name_english',
        'product_name_lenght', 'product_description_lenght', 'product_photos_qty',
        'product_weight_g', 'product_length_cm', 'product_height_cm', 'product_width_cm'
    ]].rename(columns={
        'product_name_lenght': 'product_name_length',
        'product_description_lenght': 'product_description_length'
    })


def create_dim_date(dfs: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Crea dimensión de fecha."""
    orders = dfs['orders'].copy()
    orders['order_purchase_timestamp'] = pd.to_datetime(
        orders['order_purchase_timestamp'], errors='coerce'
    )
    
    # Extraer fechas únicas
    dates = orders['order_purchase_timestamp'].dropna().dt.date.unique()
    dates = pd.to_datetime(dates)
    
    dim_date = pd.DataFrame({
        'date': dates,
        'year': dates.year,
        'quarter': dates.quarter,
        'month': dates.month,
        'day': dates.day,
        'day_of_week': dates.dayofweek,
        'month_name': dates.strftime('%B'),
        'day_name': dates.strftime('%A')
    })
    
    return dim_date.sort_values('date').reset_index(drop=True)


def create_dim_reviews(dfs: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Crea dimensión de reviews."""
    return dfs['reviews'][[
        'review_id', 'order_id', 'review_score',
        'review_comment_title', 'review_comment_message',
        'review_creation_date', 'review_answer_timestamp'
    ]].copy()


def create_fact_sales(dfs: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Crea tabla de hechos de ventas."""
    # Preparar orders
    orders = dfs['orders'].copy()
    for col in ['order_purchase_timestamp', 'order_approved_at', 
                'order_delivered_carrier_date', 'order_delivered_customer_date',
                'order_estimated_delivery_date']:
        orders[col] = pd.to_datetime(orders[col], errors='coerce')
    
    orders['order_date'] = orders['order_purchase_timestamp'].dt.date
    
    # Merge con items
    fact = orders.merge(dfs['items'], on='order_id', how='inner')
    
    # Merge con payments
    fact = fact.merge(dfs['payments'], on='order_id', how='left')
    
    # Calcular métricas
    fact['total_item_value'] = fact['price'] + fact['freight_value']
    
    # Días de entrega
    fact['delivery_days'] = (
        fact['order_delivered_customer_date'] - fact['order_purchase_timestamp']
    ).dt.total_seconds() / 86400
    
    # Entrega a tiempo
    fact['on_time_delivery'] = (
        fact['order_delivered_customer_date'] <= fact['order_estimated_delivery_date']
    ).astype(int)
    
    # Renombrar columna
    fact = fact.rename(columns={'price': 'item_price'})
    
    # Seleccionar columnas finales
    columns = [
        'order_id', 'customer_id', 'seller_id', 'product_id', 'order_date', 'order_status',
        'order_purchase_timestamp', 'order_approved_at', 'order_delivered_carrier_date',
        'order_delivered_customer_date', 'order_estimated_delivery_date',
        'order_item_id', 'item_price', 'freight_value', 'total_item_value',
        'payment_sequential', 'payment_type', 'payment_installments', 'payment_value',
        'delivery_days', 'on_time_delivery'
    ]
    
    return fact[columns].copy()


def create_star_schema_from_csv(
    config: ETLConfig,
    logger: logging.Logger
) -> Dict[str, bool]:
    """
    Crea esquema estrella directamente desde archivos CSV raw.
    No requiere PostgreSQL.
    
    Args:
        config: Configuración del ETL
        logger: Logger
        
    Returns:
        Diccionario con resultados de creación por tabla
    """
    logger.info("="*70)
    logger.info("CREANDO STAR SCHEMA DESDE CSV RAW")
    logger.info("="*70)
    
    # Crear directorio processed
    config.processed_folder.mkdir(parents=True, exist_ok=True)
    logger.info(f"Directorio de salida: {config.processed_folder}")
    
    results = {}
    
    try:
        # Cargar todos los CSV raw
        logger.info("📁 Cargando archivos raw...")
        dfs = load_raw_dataframes(config, logger)
        logger.info(f"  ✓ {len(dfs)} archivos cargados\n")
        
        # Diccionario de funciones para crear cada dimensión/hecho
        schema_functions = {
            'dim_customers': create_dim_customers,
            'dim_sellers': create_dim_sellers,
            'dim_products': create_dim_products,
            'dim_date': create_dim_date,
            'dim_reviews': create_dim_reviews,
            'fact_sales': create_fact_sales,
        }
        
        # Procesar cada tabla del star schema
        for table_name, create_func in schema_functions.items():
            try:
                logger.info(f"📊 Procesando: '{table_name}'")
                
                # Crear DataFrame
                df = create_func(dfs)
                rows, cols = df.shape
                
                logger.debug(f"  ✓ Creado: {rows:,} filas, {cols} columnas")
                
                # Exportar a CSV
                output_path = config.processed_folder / f"{table_name}.csv"
                df.to_csv(output_path, index=False, encoding='utf-8')
                
                file_size_mb = round(output_path.stat().st_size / 1024**2, 2)
                
                logger.info(
                    f"  ✓ Exportado | "
                    f"Filas: {rows:,} | "
                    f"Columnas: {cols} | "
                    f"Tamaño: {file_size_mb} MB"
                )
                
                results[table_name] = True
                
            except KeyError as e:
                logger.error(f"  ✗ Falta archivo requerido: {e}")
                results[table_name] = False
            except Exception as e:
                logger.error(f"  ✗ Error procesando '{table_name}': {e}")
                results[table_name] = False
            
            logger.info("-"*70)
        
    except Exception as e:
        logger.error(f"✗ Error cargando archivos raw: {e}")
        return {}
    
    # Resumen
    successful = sum(1 for v in results.values() if v)
    failed = len(results) - successful
    
    logger.info("="*70)
    logger.info("RESUMEN STAR SCHEMA")
    logger.info("="*70)
    logger.info(f"✓ Tablas creadas: {successful}/{len(results)}")
    if failed > 0:
        logger.warning(f"✗ Tablas con errores: {failed}")
    logger.info("="*70)
    
    return results


# ============================================================================
# ORQUESTADOR PRINCIPAL
# ============================================================================

def run_etl_pipeline(mode: str = ETLMode.HYBRID) -> int:
    """
    Orquesta el proceso completo de ETL.
    
    Args:
        mode: Modo de ejecución ('sql', 'csv', 'hybrid')
    
    Returns:
        Código de salida (0 = éxito, 1 = error)
    """
    # Inicializar configuración y logging
    config = ETLConfig()
    logger = setup_logging(config)
    
    logger.info("="*70)
    logger.info("INICIANDO PIPELINE ETL - BRAZILIAN E-COMMERCE OLIST")
    logger.info("="*70)
    logger.info(f"Modo de ejecución: {mode.upper()}")
    logger.info(f"Directorio de datos: {config.csv_folder}")
    
    if mode in [ETLMode.SQL_ONLY, ETLMode.HYBRID]:
        logger.info(f"Base de datos: {config.db_name}")
    if mode in [ETLMode.CSV_ONLY, ETLMode.HYBRID]:
        logger.info(f"Directorio procesados: {config.processed_folder}")
    
    logger.info(f"Total de tablas raw: {len(TABLE_FILE_MAPPING)}")
    logger.info("-"*70)
    
    # Crear conexión a base de datos (solo si es necesario)
    engine = None
    if mode in [ETLMode.SQL_ONLY, ETLMode.HYBRID]:
        engine = create_database_engine(config, logger)
        if not engine:
            if mode == ETLMode.SQL_ONLY:
                logger.critical("No se pudo establecer conexión a la base de datos. Abortando.")
                return 1
            else:
                logger.warning("No se pudo conectar a PostgreSQL. Solo se generarán CSV procesados.")
                mode = ETLMode.CSV_ONLY
    
    # Verificar que existe el directorio de datos
    if not config.csv_folder.exists():
        logger.critical(f"El directorio de datos no existe: {config.csv_folder}")
        return 1
    
    start_time = datetime.now()
    results = {'success': 0, 'failed': 0, 'skipped': 0}
    csv_results = {}
    
    # ========== FASE 1: CARGA A SQL (si aplica) ==========
    if mode in [ETLMode.SQL_ONLY, ETLMode.HYBRID] and engine:
        logger.info("")
        logger.info("═" * 70)
        logger.info("FASE 1: CARGA DE DATOS RAW A POSTGRESQL")
        logger.info("═" * 70)
        
        for table_name, file_name in TABLE_FILE_MAPPING.items():
            file_path = config.csv_folder / file_name
            
            if not file_path.exists():
                logger.warning(f"⊘ Archivo omitido (no existe): {file_name}")
                results['skipped'] += 1
                continue
            
            success = load_table_to_database(
                table_name=table_name,
                file_path=file_path,
                engine=engine,
                config=config,
                logger=logger
            )
            
            if success:
                results['success'] += 1
            else:
                results['failed'] += 1
            
            logger.info("-"*70)
        
        # Resumen de carga SQL
        logger.info("="*70)
        logger.info("RESUMEN CARGA SQL")
        logger.info("="*70)
        logger.info(f"✓ Tablas cargadas: {results['success']}")
        logger.info(f"✗ Tablas con errores: {results['failed']}")
        logger.info(f"⊘ Tablas omitidas: {results['skipped']}")
        logger.info("="*70)
    
    # ========== FASE 2: GENERAR CSV PROCESADOS (si aplica) ==========
    if mode in [ETLMode.CSV_ONLY, ETLMode.HYBRID]:
        logger.info("")
        logger.info("═" * 70)
        logger.info("FASE 2: GENERACIÓN DE STAR SCHEMA (CSV PROCESADOS)")
        logger.info("═" * 70)
        
        csv_results = create_star_schema_from_csv(config, logger)
    
    # ========== RESUMEN FINAL ==========
    elapsed_time = datetime.now() - start_time
    
    logger.info("")
    logger.info("="*70)
    logger.info("RESUMEN FINAL DE EJECUCIÓN")
    logger.info("="*70)
    
    if mode in [ETLMode.SQL_ONLY, ETLMode.HYBRID] and engine:
        logger.info(f"📊 Carga SQL: {results['success']} exitosas, {results['failed']} fallidas")
    
    if mode in [ETLMode.CSV_ONLY, ETLMode.HYBRID]:
        csv_success = sum(1 for v in csv_results.values() if v)
        csv_failed = len(csv_results) - csv_success if csv_results else 0
        logger.info(f"📁 CSV procesados: {csv_success} exitosos, {csv_failed} fallidos")
    
    logger.info(f"⏱ Tiempo total: {elapsed_time}")
    logger.info("="*70)
    
    # Cerrar conexión SQL si existe
    if engine:
        engine.dispose()
    
    # Determinar código de salida
    if results['failed'] > 0:
        logger.warning("⚠ ETL completado con errores")
        return 1
    else:
        logger.info("✓ ETL COMPLETADO EXITOSAMENTE")
        return 0


# ============================================================================
# PUNTO DE ENTRADA
# ============================================================================

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(
        description='ETL Pipeline para Brazilian E-Commerce',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Ejemplos de uso:
  python 01_etl_loader.py --mode hybrid    # Carga a SQL + genera CSV procesados (por defecto)
  python 01_etl_loader.py --mode sql       # Solo carga a PostgreSQL
  python 01_etl_loader.py --mode csv       # Solo genera CSV procesados (sin PostgreSQL)
        """
    )
    
    parser.add_argument(
        '--mode',
        type=str,
        choices=[ETLMode.SQL_ONLY, ETLMode.CSV_ONLY, ETLMode.HYBRID],
        default=ETLMode.HYBRID,
        help=f"Modo de ejecución (default: {ETLMode.HYBRID})"
    )
    
    args = parser.parse_args()
    
    exit_code = run_etl_pipeline(mode=args.mode)
    sys.exit(exit_code)