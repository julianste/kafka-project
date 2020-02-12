"""Defines core consumer functionality"""
import logging
import json

import confluent_kafka
from confluent_kafka import Consumer, OFFSET_BEGINNING
from confluent_kafka.avro import AvroConsumer
from confluent_kafka.avro.serializer import SerializerError
from tornado import gen


logger = logging.getLogger(__name__)


class KafkaConsumer:
    """Defines the base kafka consumer class"""

    def __init__(
        self,
        topic_name_pattern,
        message_handler,
        is_avro=True,
        offset_earliest=False,
        sleep_secs=1.0,
        consume_timeout=0.1,
    ):
        """Creates a consumer object for asynchronous use"""
        self.topic_name_pattern = topic_name_pattern
        self.message_handler = message_handler
        self.sleep_secs = sleep_secs
        self.consume_timeout = consume_timeout
        self.offset_earliest = offset_earliest
        self.subscribed = False
        #
        #
        # Configure the broker properties below. Make sure to reference the project README
        # and use the Host URL for Kafka and Schema Registry!
        #
        #
        self.broker_properties = {
            'bootstrap.servers': "PLAINTEXT://kafka0:19092,PLAINTEXT://kafka1:19092,PLAINTEXT://kafka2:19092",
            "group.id": "cta-1",
            "session.timeout.ms": 20000,
            "heartbeat.interval.ms": 1500,
            "max.poll.interval.ms": 60000,
            # 'debug': 'broker'
            }
        if self.offset_earliest:
            self.broker_properties['auto.offset.reset'] = "earliest"

        # Create the Consumer, using the appropriate type.
        if is_avro is True:
            self.broker_properties["schema.registry.url"] = "http://schema-registry:8081"
            self.consumer = AvroConsumer(self.broker_properties)
        else:
            self.consumer = Consumer(self.broker_properties)

        #
        #
        # Configure the AvroConsumer and subscribe to the topics. Make sure to think about
        # how the `on_assign` callback should be invoked.
        #
        #
        self.consumer.subscribe([self.topic_name_pattern], on_assign=self.on_assign)

    def on_assign(self, consumer, partitions):
        """Callback for when topic assignment takes place"""
        # TODO: If the topic is configured to use `offset_earliest` set the partition offset to
        # the beginning or earliest
        self.subscribed = True
        logger.info("in on_assing, offset is "+ str(self.offset_earliest))
        if self.offset_earliest:
            logger.info("set offset to beginning...")
            for partition in partitions:
                partition.offset = OFFSET_BEGINNING

        logger.info("partitions assigned for %s", self.topic_name_pattern)
        consumer.assign(partitions)

    async def consume(self):
        # while not self.subscribed:
        #     gen.sleep(self.sleep_secs)
        """Asynchronously consumes data from kafka topic"""
        while True:
            num_results = 1
            while num_results > 0:
                num_results = self._consume()
            await gen.sleep(self.sleep_secs)

    def _consume(self):
        """Polls for a message. Returns 1 if a message was received, 0 otherwise"""
        logger.debug("consuming message")
        message = self.consumer.poll(timeout=self.consume_timeout)
        if message is None:
            logger.debug("no message received!")
            return 0
            self.close()
        elif message.error() is not None:
            raise Exception("error during consumer poll" + message.error().str())
            return 0
            self.close()
        else:
            self.message_handler(message)
            return 1

    def close(self):
        """Cleans up any open kafka consumers"""
        logger.info("closing consumer...")
        self.consumer.close()
