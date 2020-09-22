# from confluent_kafka import Consumer
#
#
# c = Consumer({
#     'bootstrap.servers': 'b-1.bright-demo-mks-cluste.t98hyr.c3.kafka.us-west-2.amazonaws.com:9094',
#     'group.id': 'mygroup',
#     'auto.offset.reset': 'earliest',
#     'security.protocol':'SSL'
# })
#
# t = c.list_topics()
# print(t)
#
# c.subscribe(['bright_topic'])
# print("done subscribing")
# # while True:
# #     msg = c.poll(1.0)
# #
# #     if msg is None:
# #         continue
# #     if msg.error():
# #         print("Consumer error: {}".format(msg.error()))
# #         continue
# #
# #     print('Received message: {}'.format(msg.value().decode('utf-8')))
#
# c.close()

from kafka import KafkaConsumer

consumer = KafkaConsumer('my-kafka-topic',
                         group_id='group1',
                         bootstrap_servers=['localhost:9092'])

consumer.subscribe(topics='my-kafka-topic')

for message in consumer:
    # message value and key are raw bytes -- decode if necessary!
    # e.g., for unicode: `message.value.decode('utf-8')`

    try:
        print ("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
                                          message.offset, message.key,
                                          message.value))

    except Exception as e:
        print(str(e))


