from components.rabbitmq_operations import connect_rabbit, setup_queue, callback
import pika

connection_parameters = connect_rabbit()

connection = pika.BlockingConnection(connection_parameters)
channel = connection.channel()

# Setup Queue
queue_name = "Letseng"
setup_queue(channel, queue_name)

#! NOTE THE AUTO_ACK SETTING
channel.basic_consume(
    queue=queue_name, on_message_callback=callback, auto_ack=False)

# Start consuming
print("Press 'Enter' to stop...")
channel.start_consuming()

# Here we stop consuming and close the connection when 'Enter' is pressed.
connection.close()
