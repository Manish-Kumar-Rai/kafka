from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer
import pandas as pd
import time


def delivery_report(err,msg):
    if err is not None:
        print("Delivery failed for User record {}: {}".format(msg.key(),err))
        return
    print('User record {} successfully produced to {} [{}] at offset {}'.format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))
    print("==========================================================")

#Define Kafka Configuration
kafka_config = {
    'bootstrap.servers':'your_kafka_bootstrap_server',
    'sasl.mechanisms':'PLAIN',
    'security.protocol':'SASL_SSL',
    'sasl.username':'api_key',
    'sasl.password':'secret_key',
}

# Create a Schema Registry client
schema_registry_client = SchemaRegistryClient({
    'url':'schema_registry_url(public_end_point)',
    'basic.auth.user.info':'{}:{}'.format('api_key_for_schema_registry','secret_key_for_schema_registry')
})

# Fetch the latest Avro schema for the value
subject_name = 'retail_data_dev-value'
schema_str = schema_registry_client.get_latest_version(subject_name).schema.schema_str

# Create Avro Serializer for the value
key_serializer = StringSerializer('utf_8')
avro_serializer = AvroSerializer(schema_registry_client,schema_str)


#Define the SerializingProducer
producer = SerializingProducer({
    'bootstrap.servers': kafka_config['bootstrap.servers'],
    'security.protocol': kafka_config['security.protocol'],
    'sasl.mechanisms': kafka_config['sasl.mechanisms'],
    'sasl.username': kafka_config['sasl.username'],
    'sasl.password': kafka_config['sasl.password'],
    'key.serializer':key_serializer,
    'value.serializer':avro_serializer
})

# Load the CSV data into a pandas DataFrame
df = pd.read_csv('retail_data.csv')
df = df.fillna('null')


# Iterate over DataFrame rows and produce to Kafka
for index, row in df.iterrows():
    # Create a dictionary from the row values
    data_value = row.to_dict()
    # print(f"index:{index},\nvalue:{data_value}")

    producer.produce(
        topic='retail_data_dev',
        key=str(index),
        value=data_value,
        on_delivery=delivery_report
    )

    producer.flush()
    time.sleep(2)

print("All Data Records Produced Successfully")