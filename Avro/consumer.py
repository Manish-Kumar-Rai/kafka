from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import StringDeserializer


#Define Kafka Configuration
kafka_config = {
    'bootstrap.servers':'your_kafka_bootstrap_server',
    'sasl.mechanisms':'PLAIN',
    'security.protocol':'SASL_SSL',
    'sasl.username':'api_key',
    'sasl.password':'secret_key',
    'group.id':'group1',
    'auto.offset.reset':'latest'  # Start reading from the latest message
}

# Create a Schema Registry client
schema_registry_client = SchemaRegistryClient({
    'url':'schema_registry_url(public_end_point)',
    'basic.auth.user.info':'{}:{}'.format('api_key_for_schema_registry','secret_key_for_schema_registry')
})

# Fetch the latest Avro schema for the value
subject_name = 'retail_data_dev-value'
schema_str = schema_registry_client.get_latest_version(subject_name).schema.schema_str

# Create Avro Deserializer for the value
key_deserializer = StringDeserializer('utf_8')
avro_deserializer = AvroDeserializer(schema_registry_client, schema_str)

# Define the DeserializingConsumer
consumer = DeserializingConsumer({
    'bootstrap.servers': kafka_config['bootstrap.servers'],
    'security.protocol': kafka_config['security.protocol'],
    'sasl.mechanisms': kafka_config['sasl.mechanisms'],
    'sasl.username': kafka_config['sasl.username'],
    'sasl.password': kafka_config['sasl.password'],
    'key.deserializer': key_deserializer,
    'value.deserializer': avro_deserializer,
    'group.id': kafka_config['group.id'],
    'auto.offset.reset': kafka_config['auto.offset.reset']
    # 'enable.auto.commit': True,
    # 'auto.commit.interval.ms': 5000 # Commit every 5000 ms, i.e., every 5 seconds
})

# Subscribe to the 'retail_data' topic
consumer.subscribe(['retail_data_dev'])

try:
    while True:
        msg = consumer.poll(1.0) # How many seconds to wait for message

        if msg is None:
            continue
        if msg.error():
            print("Consumer error: {}".format(msg.error()))
            continue

        print('Successfully consumed record with key {} and value {}'.format(msg.key(), msg.value()))

except KeyboardInterrupt:
    pass
finally:
    consumer.close()
    print("Consumer closed.")