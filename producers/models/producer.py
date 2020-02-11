"""Producer base-class providing common utilites and functionality"""
import logging
import time
import datetime

from confluent_kafka import avro
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroProducer

logger = logging.getLogger(__name__)


class Producer:
    """Defines and provides common functionality amongst Producers"""

    # Tracks existing topics across all Producer instances
    existing_topics = set([])

    def __init__(
        self,
        topic_name,
        key_schema,
        value_schema=None,
        num_partitions=1,
        num_replicas=1,
    ):
        """Initializes a Producer object with basic settings"""
        self.topic_name = topic_name
        self.key_schema = key_schema
        self.value_schema = value_schema
        self.num_partitions = num_partitions
        self.num_replicas = num_replicas
        # Configure the broker properties below. Make sure to reference the project README
        # and use the Host URL for Kafka and Schema Registry!
        #
        #
        self.broker_properties = {
            'bootstrap.servers': "PLAINTEXT://kafka0:19092,PLAINTEXT://kafka1:19092,PLAINTEXT://kafka2:19092",
            'on_delivery': self.delivery_report,
            'schema.registry.url': 'http://schema-registry:8081'
            }

        # If the topic does not already exist, try to create it
        if self.topic_name not in Producer.existing_topics:
            self.create_topic()
            Producer.existing_topics.add(self.topic_name)

        # Configure the AvroProducer
        self.producer = AvroProducer(self.broker_properties, default_key_schema=self.key_schema, default_value_schema=self.value_schema
        )

    def create_topic(self):
        """Creates the producer topic if it does not already exist"""
        #
        #
        # code that creates the topic for this producer if it does not already exist on
        # the Kafka Broker.
        #
        #
        print("Create topic " + self.topic_name)
        a = AdminClient({'bootstrap.servers': "PLAINTEXT://kafka0:19092,PLAINTEXT://kafka1:19092,PLAINTEXT://kafka2:19092"})
        futures = a.create_topics([NewTopic(self.topic_name, 3, 2)])

        for topic, future in futures.items():
            try:
                future.result()
                print("topic created")
            except Exception as e:
                print(f"failed to create topic {self.topic_name}: {e}")

    def time_millis(self):
        return int(round(time.time() * 1000))

    def _unix_time_millis(self, dt):
        epoch = datetime.datetime.utcfromtimestamp(0)
        return int((dt - epoch).total_seconds() * 1000.0)

    def close(self):
        """Prepares the producer for exit by cleaning up the producer"""
        #
        #
        # cleanup code for the Producer here
        #
        #
        self.producer.flush()

    def time_millis(self):
        """Use this function to get the key for Kafka Events"""
        return int(round(time.time() * 1000))

    def delivery_report(err, msg):
        """ Called once for each message produced to indicate delivery result.
            Triggered by poll() or flush(). """
        if err is not None:
            print('Message delivery failed: {}'.format(err))
        else:
            print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))