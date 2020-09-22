# from kafka import KafkaProducer, KafkaAdminClient
# from kafka.cluster import ClusterMetadata
# import json, time
#
# def json_serializer(data):
#     return json.dumps(data).encode("utf-8")
#
# def on_send_error(excp):
#     print('I am an errback'+str(excp))
#     # handle exception
#
# if __name__ == "__main__":
#
#     data = '''
#     {
#         "first": "Hello World!"
#     }
#
#     '''
#
#     print("--------")
#     producer = KafkaProducer(
#         bootstrap_servers='b-1.bright-demo-mks-cluste.t98hyr.c3.kafka.us-west-2.amazonaws.com:9094',
#         value_serializer=lambda x: json.dumps(x).encode('utf-8'),
#         api_version=(2, 1, 1),
#         ssl_check_hostname=True,
#         ssl_cafile='/tmp/ssl22/CARoot.pem',
#
#         security_protocol='SSL'
#         #ssl_certfile='/tmp/ssl100/certificate.pem'
#         #ssl_keyfile='key.pem'
#     )
#
#
#     print(producer)
#
#     print("data")
#     try:
#         topic = "bright_topic"
#         producer.send(topic, b'some_message_bytes').add_errback(on_send_error)
#
#     except Exception as e:
#         print("excep::::")
#         print(str(e))
#     print("sent")
#     producer.flush()

# from confluent_kafka import Producer, Consumer
#
# def delivery_report(err, msg):
#     """ Called once for each message produced to indicate delivery result.
#         Triggered by poll() or flush(). """
#     if err is not None:
#         print('Message delivery failed: {}'.format(err))
#     else:
#         print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))
#
#
# if __name__ == "__main__":
#
#     data = ''''''
#
#     print("data")
#     try:
#
#         p = Producer({
#             'bootstrap.servers': 'b-1.bright-demo-mks-cluste.t98hyr.c3.kafka.us-west-2.amazonaws.com:9092',
#             'security.protocol': 'ssl',
#              'ssl.ca.location': '/tmp/ssl-kafka/CARoot.pem',
#             'ssl.certificate.location': '/tmp/ssl-kafka/certificate.pem',
#              'ssl.key.location': '/tmp/ssl-kafka/key_1.key'
#         })
#
#         # Trigger any available delivery report callbacks from previous produce() calls
#         p.poll(0)
#
#         # Asynchronously produce a message, the delivery report callback
#         # will be triggered from poll() above, or flush() below, when the message has
#         # been successfully delivered or failed permanently.
#
#         p.produce("bright_topic", "my_value", "my_key")
#
#         #p.produce('bright_topic', data, callback=delivery_report)
#     except Exception as e:
#         print("excep::::")
#         print(str(e))
#     print("sent")
#     # Wait for any outstanding messages to be delivered and delivery report
#     # callbacks to be triggered.


import json
from time import sleep

from kafka import KafkaProducer


def json_serializer(data):
    return json.dumps(data).encode("utf-8")


def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))


def on_send_error(excp):
    print('I am an errback' + str(excp))
    # handle exception


if __name__ == "__main__":

    data = "Some data"

    producer = KafkaProducer(
        bootstrap_servers='localhost:9092,localhost:9093,localhost:9094'
    )

    try:
        topic = "topic3"

        for i in range(1, 5):
            s = {"-": i}
            sleep(0.01)
            s = json.dumps(s)
            producer.send(topic, value=s.encode('utf-8')).add_errback(on_send_error)

    except Exception as e:
        print("excep::::" + str(e))

    producer.flush()
