from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import requests
import logging
import psycopg2
from airflow.hooks.base import BaseHook

logger = logging.getLogger(__name__)
COINS = ['bitcoin', 'ethereum']

def check_connection():
    try:
        response = requests.get('https://api.coingecko.com/api/v3/ping', timeout=10)
        logger.info(f'API ответил: {response.json()}')
        return 'API OK'
    except Exception as e:
        logger.error(f'Ошибка API: {e}')
        raise

def get_prices():
    try:
        url = 'https://api.coingecko.com/api/v3/simple/price'
        params = {
            'ids': ','.join(COINS),
            'vs_currencies': 'usd',
            'include_24hr_change': 'true',
            'include_market_cap': 'true',
            'include_24hr_vol': 'true'
        }
        response = requests.get(url, params=params, timeout=10)
        data = response.json()
        logger.info(f'Получены данные: {data}')
        return data
    except Exception as e:
        logger.error(f'Ошибка получения цен: {e}')
        raise

def save_to_postgres(**context):
    ti = context['ti']
    data = ti.xcom_pull(task_ids='get_prices')
    
    # Подключение к БД
    conn = BaseHook.get_connection('postgres_dwh')
    pg_conn = psycopg2.connect(
        host=conn.host,
        port=conn.port,
        database=conn.schema,
        user=conn.login,
        password=conn.password
    )
    
    cursor = pg_conn.cursor()
    snapshot_time = datetime.utcnow()
    
    for coin_id, coin_data in data.items():
        cursor.execute("""
            INSERT INTO raw.crypto_snapshot 
            (snapshot_time, coin_id, price_usd, market_cap_usd, volume_24h_usd, price_change_24h)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (
            snapshot_time,
            coin_id,
            coin_data.get('usd'),
            coin_data.get('usd_market_cap'),
            coin_data.get('usd_24h_vol'),
            coin_data.get('usd_24h_change')
        ))
        
        
        cursor.execute("""
            INSERT INTO dds.dim_coin (coin_code, coin_name, symbol)
            VALUES (%s, %s, %s)
            ON CONFLICT (coin_code) DO NOTHING
        """, (coin_id, coin_id.capitalize(), coin_id[:3].upper()))
        
        cursor.execute("SELECT coin_id FROM dds.dim_coin WHERE coin_code = %s", (coin_id,))
        result = cursor.fetchone()
        if result:
            coin_db_id = result[0]
            
            cursor.execute("""
                INSERT INTO dds.fact_market_data 
                (coin_id, snapshot_time, price_usd, market_cap_usd, volume_24h_usd, price_change_24h)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (
                coin_db_id,
                snapshot_time,
                coin_data.get('usd'),
                coin_data.get('usd_market_cap'),
                coin_data.get('usd_24h_vol'),
                coin_data.get('usd_24h_change')
            ))
    
    # Обновляем витрину
    cursor.execute("""
        INSERT INTO dm.market_summary (snapshot_time, total_market_cap, btc_price, total_volume)
        SELECT 
            %s,
            COALESCE(SUM(f.market_cap_usd), 0),
            MAX(CASE WHEN c.coin_code = 'bitcoin' THEN f.price_usd END),
            COALESCE(SUM(f.volume_24h_usd), 0)
        FROM dds.fact_market_data f
        JOIN dds.dim_coin c ON f.coin_id = c.coin_id
        WHERE f.snapshot_time = %s
    """, (snapshot_time, snapshot_time))
    
    pg_conn.commit()
    cursor.close()
    pg_conn.close()
    logger.info(f'Данные сохранены в БД для {len(data)} монет')
    return 'Saved'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'crypto_simple',
    default_args=default_args,
    description='Простой крипто-пайплайн',
    schedule_interval='*/10 * * * *',
    catchup=False,
    tags=['crypto'],
)

t1 = PythonOperator(task_id='check_api', python_callable=check_connection, dag=dag)
t2 = PythonOperator(task_id='get_prices', python_callable=get_prices, dag=dag)
t3 = PythonOperator(task_id='save_to_db', python_callable=save_to_postgres, provide_context=True, dag=dag)

t1 >> t2 >> t3