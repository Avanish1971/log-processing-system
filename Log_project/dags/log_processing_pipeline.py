from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from confluent_kafka import Consumer,KafkaException
from elasticsearch import Elasticsearch
import json
import logging
import boto3

logger=logging.getLogger(__name__)

