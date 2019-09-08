import sys
import pika

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost', 5677))
channel = connection.channel()
channel.queue_declare(queue='hello')

message = ' '.join(sys.argv[1:]) or "Hello World!"
channel.basic_publish(exchange='',
                      routing_key='hello',
                      body=message)

channel.basic.ack
print(" [x] Sent %r" % message)
