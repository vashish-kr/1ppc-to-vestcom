# Databricks notebook source
# Databricks notebook source
spark.conf.set("fs.azure.account.auth.type.mxcoredataprodstrg.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.mxcoredataprodstrg.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.mxcoredataprodstrg.dfs.core.windows.net", "18cc7074-141f-48e3-8704-3fbdb42ea7b7")
spark.conf.set("fs.azure.account.oauth2.client.secret.mxcoredataprodstrg.dfs.core.windows.net", dbutils.secrets.get(scope = "MERCH-MXI-PROD-DBX-SCOPE", key = "merch-mxi-prod-dmp-p-secret"))
spark.conf.set("fs.azure.account.oauth2.client.endpoint.mxcoredataprodstrg.dfs.core.windows.net", "https://login.microsoftonline.com/8331e14a-9134-4288-bf5a-5e2c8412f074/oauth2/token")

# COMMAND ----------

dbutils.widgets.text("kafka_subject", "", "Kafka Subject")
dbutils.widgets.text("kafka_secret", "", "Kafka Secret")
dbutils.widgets.text("kafka_schema_registry_address", "", "Kafka Schema Registry Address")
dbutils.widgets.text("client_id", "", "Client ID")
dbutils.widgets.text("client_secret", "", "Client Secret")
dbutils.widgets.text("kafka_bootstrap_servers", "", "Kafka Bootstrap Servers")
dbutils.widgets.text("tenant_id", "", "Tenant ID")
dbutils.widgets.text("group_id", "", "Group ID")
dbutils.widgets.text("subscribe_topic", "", "Subscribe Topic")
dbutils.widgets.text("checkpoint_path", "", "Checkpoint Path")
dbutils.widgets.text("table_name", "", "Table Name")

# COMMAND ----------

import requests
from requests.auth import HTTPBasicAuth
import json

class KafkaConfig:
    def __init__(self):
        # Retrieve values from Databricks widgets
        self.kafka_subject = dbutils.widgets.get("kafka_subject")
        self.kafka_secret = dbutils.widgets.get("kafka_secret")
        self.kafka_schema_registry_address = dbutils.widgets.get("kafka_schema_registry_address")
        self.tenant_id = dbutils.widgets.get("tenant_id")
        self.client_id = dbutils.widgets.get("client_id")
        self.client_secret = dbutils.widgets.get("client_secret")
        self.kafka_bootstrap_servers = dbutils.widgets.get("kafka_bootstrap_servers")
        self.table_name = dbutils.widgets.get("table_name")
        self.checkpoint_path = dbutils.widgets.get("checkpoint_path")

        self.group_id = dbutils.widgets.get("group_id")
        self.subscribe_topic = dbutils.widgets.get("subscribe_topic")
        
        # Initialize Config attributes
        self.config = Config(self.tenant_id, self.client_id, self.client_secret, self.kafka_bootstrap_servers,
                             self.kafka_subject, self.kafka_secret, self.kafka_schema_registry_address,self.subscribe_topic,self.table_name,self.checkpoint_path)
        
    def get_schema_from_registry(self):
        return self.config.getSchema()
class Config:
    def __init__(self, tenant_id, client_id, client_secret, bootstrap_server,
                 subject, schema_registry_secret, schema_registry_address,subscribe_topic,table_name,checkpoint_path):
        self.topic_name =subscribe_topic
        self.tenant_id = tenant_id
        self.auth_url = f"https://login.microsoftonline.com/{self.tenant_id}/"
        self.client_id = client_id
        self.client_secret = client_secret
        self.table_name = table_name
        self.checkpoint_path = checkpoint_path

        self.bootstrap_server = bootstrap_server
        self.subject = subject
        self.schema_registry_secret = schema_registry_secret
        self.schema_registry_address = schema_registry_address
        
        self.schema_registry_options = {
                "confluent.schema.registry.basic.auth.credentials.source": 'USER_INFO',
                "confluent.schema.registry.basic.auth.user.info": self.schema_registry_secret,
                "mode":"PERMISSIVE"
            }
        self.conf = {
            "kafka.bootstrap.servers": self.bootstrap_server,
            "kafka.sasl.mechanism": "OAUTHBEARER",
            "kafka.security.protocol": "SASL_SSL",
            "kafka.sasl.jaas.config": (
                'kafkashaded.org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required '
                f'authUrl="{self.auth_url}" '
                f'appId="{self.client_id}" '
                f'appSecret="{self.client_secret}";'
            ),
            "kafka.sasl.login.callback.handler.class": 'com.kroger.streaming.ext.azure.oauth.EventHubsOAuthHandler',
            "subscribe": self.topic_name,
            "startingOffsets": "earliest",
            "mode": "PERMISSIVE",
            "request.timeout.ms": "120000",
            "session.timeout.ms": "120000"
        }
        
    def getSchema(self):
        response = requests.get(
            f"{self.schema_registry_address}/subjects/{self.subject}/versions/latest",
            auth=HTTPBasicAuth(
                self.schema_registry_secret[:16], self.schema_registry_secret[17:]
            ),
        )
        return json.dumps(
            json.loads(json.loads(response.content.decode("utf-8"))["schema"]), indent=4
        )
    



# COMMAND ----------

from pyspark.sql.functions import current_timestamp



def create_managed_table(spark, table_name, schema, primary_key_column):
    try:
        df_existing = spark.read.table(table_name)
        table_exists = True
    except Exception as e:
        table_exists = False

    if not table_exists:
        df = spark.createDataFrame([], schema)
        df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(table_name)
        spark.sql(f"ALTER TABLE {table_name} ALTER COLUMN {primary_key_column} SET NOT NULL")
        spark.sql(f"ALTER TABLE {table_name} ADD CONSTRAINT {primary_key_column}_pk PRIMARY KEY ({primary_key_column})")

        print(f"Managed table {table_name} has been created with primary key on {primary_key_column}.")
    else:
        print(f"Table {table_name} already exists. No action taken.")


def add_current_timestamp(data_frame, column_name):
    return data_frame.withColumn(column_name, current_timestamp())
