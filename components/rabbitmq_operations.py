import pika
import json

# RabbitMQ Connection settings
def connect_rabbit():
    credentials = pika.PlainCredentials('concat', 'C0nc@t!@#$')
    connection_parameters = pika.ConnectionParameters(
        host='sensservicebus.vsvs.co.za',
        virtual_host='Letseng',
        credentials=credentials
    )
    return connection_parameters

def setup_queue(channel, queue_name):
    channel.queue_declare(queue=queue_name, durable=False,
                          exclusive=False, auto_delete=False)

def callback(body):
    from components.database_operations import insert_into_db

    raw_message = body.decode('utf-8')
    # ~ print("Received raw message:", raw_message)

    outer_data = json.loads(raw_message)
    detailed_data = json.loads(outer_data['message'])

    if detailed_data:
        msgtype = "{}:{}".format(
            outer_data['messagetype'], outer_data['category'])
        if msgtype == "Hours:Engine Hours":
            insert_into_db("EngineHoursTable", detailed_data)
        elif msgtype == "Production:Loads":
            insert_into_db("LoadsTable", detailed_data)
        elif msgtype == "Production:Loads Per Hour":
            insert_into_db("LoadsPerHourTable", detailed_data)
        elif msgtype == "Operational:Downtime":
            insert_into_db("DowntimeTable", detailed_data)
        else:
            # Unrecognized message
            pass
