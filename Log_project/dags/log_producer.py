from airflow import DAG
from airflow.operators.python import PythonOperator
from confluent_kafka import Producer
from faker import Faker
import logging
from datetime import datetime, timedelta
import json
import boto3
import random

fake= Faker()
logger= logging.getLogger(__name__)

def create_kafka_producer(config):
    return Producer(config)


def generate_log():
    method=['GET','POST','PUT','DELETE']
    endpoint=['/api/users','/home','/about','/contact','/services']
    status_code=[200,201,400,401,403,404,500]

    user_agent=[
        'Mozila/5.0 (iphone; cpu iphone os 14_6 like Mac os X)',
        'Mozila/5.0 (x11; linux x86_64)',
        'Mozila/5.0 (windows nt 10.0; win64; x64)',
        'Mozila/5.0 (Macintosh; Intel Mac os X 10_15_7)',
        'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Mozila/5.0 (Linux; Android 10; SM-G975F) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Mobile Safari/537.36'
    ]

    referrence=['https://example.com','https://google.com','-','https://bing.com','https://yahoo.com']

    ip=fake.ipv4()
    timestamp=datetime.now().strftime('%d/%b/%Y:%H:%M:%S %z')
    method=random.choice(method)
    endpoint=random.choice(endpoint)
    status=random.choice(statuses)
    size=random.randint(100,5000)
    referre=random.choice(referrers)
    user_agent=random.choice(user_agent)

    log_entry=(
        f'{ip} - - [{timestamp}] "{method} {endpoint} HTTP/1.1" {status} {size} "{referre}" "{user_agent}"'
    )

    return log_entry

def delivery_report(err,msg):
    if err is not None:
        logger.error(f"Message delivery failed: {err}")
    else:
        logger.info(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

def get_secret(secret_name,region_name='us-east-1'):
    # Placeholder for secret retrieval logic
    session=boto3.session.Session()
    client=session.client(service_name='secretsmanager',region_name=region_name)
    try:
        response = client.get_secret_value(SecretId=secret_name)
        return json.loads(response['SecretString'])
    except Exception as e:
        logger.error(f"secret retrieval error:{e}")
        raise

def produce_log(**context):
    secret=get_secret('mivaaa secreat v2')
    kafka_config={
        'bootstrp.server':secret['KAFKA_BOOTSTRP_SERVERS'],
        'security_protocol':'SASL_SSL',
        'sasl_mechanism':'PLAIN',
        'sasl_username':secret['KAFKA_USERNAME'],
        'sasl_password':secret['KAFKA_PASSWORD'],
        'session.timeout.ms':50000
    }

    producer=create_kafka_producer(kafka_config)
    topic='billion_website_logs'

    for _ in range(15000):
        log=generate_log()
        try:
            producer.produce(topic,log.encode('utf-8'), on_delivery=delivery_report)
            produce.flush()
        except Exception as e:
            logger.error(f"Error producing log: {e}")
            raise
    
    logger.info(f"produce 150000 logs to topic {topic}")
            
default_args = {
    'owner': 'Data Master',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag= DAG(
    dag_id='log_producer',
    default_args=default_args,
    description='Generate and produce synthetic logs',
    schedule_interval='*/5 * * * *',
    start_date=datetime(2026, 4, 15),
    catchup=False,
    tags=['log','kafka','production']
)

produce_log_talk=PythonOperator(
    talk_id='produce_log',
    python_callable=produce_log,
    dag=dag
)
