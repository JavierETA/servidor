# example_consumer.py
import pika, os, csv
from datetime import datetime
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

token = "OixuRs8ScWhsR_DaGHel9RVpdrZiqsVnH7e9_D9TnwWId1sRw6Eq7HP_2meYEJ-Smt1JK4ePQp0fBpe0h4U2bg=="
org = "Taller3"
bucket = "Dispositivo1"
client = InfluxDBClient(url="http://20.51.187.11:8086", token=token)

def envia_DB(a):
    dato = float(a)
    write_api = client.write_api(write_options=SYNCHRONOUS)
    point = Point("Mediciones").tag("Ubicacion", "Santa Marta").field("Humedad", dato).time(datetime.utcnow(), WritePrecision.NS)
    write_api.write(bucket, org, point)
    return

def process_function(msg):
  mesage = msg.decode("utf-8")
  print(mesage)
  envia_DB(mesage)
  return

while 1:
  url = os.environ.get('CLOUDAMQP_URL', 'amqp://guest:guest@rabbit:5672/%2f')
  params = pika.URLParameters(url)
  connection = pika.BlockingConnection(params)
  channel = connection.channel() # start a channel
  channel.queue_declare(queue='dato') # Declare a queue
  # create a function which is called on incoming messages
  def callback(ch, method, properties, body):
    process_function(body)

  # set up subscription on the queue
  channel.basic_consume('dato',
    callback,
    auto_ack=True)

  # start consuming (blocks)
  channel.start_consuming()
  connection.close()
