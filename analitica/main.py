import pika
import os
import math
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

class Analytics():
    max_value = -math.inf
    min_value = math.inf

    #### codigo para configurar la lectura 

    #### valor maximo 
    def add_max_value(self, _medida):
        if _medida > self.max_value:
            print("New max", flush=True)
            self.max_value = _medida
    ###· valor minimo        
    def add_min_value(self, _medida):
        if _medida < self.min_value:
            print("New min", flush=True)
            self.min_value = _medida


    def take_measurement(self, _mensaje):
        mensaje = _mensaje.split("=")
        medida = float(mensaje[-1])
        print("medida {}".format(medida))
        self.add_max_value(medida)
        self.add_min_value(medida)
        








if __name__ == '__main__':
    #variable global 
    analitica = Analytics()

    def callback(ch, method, properties, body):
        global analitica
        mensaje = body.decode("utf-8")
        #print("mensaje del servidor {}".format(mensaje), flush=True)
        analitica.take_measurement(mensaje)

    url = os.environ.get('AMQP_URL','amqp://user:pass@rabbit:5672/%2f')
    params = pika.URLParameters(url)
    connection = pika.BlockingConnection(params)
    channel = connection.channel()

    channel.queue_declare(queue='pasos')
    channel.queue_bind(exchange = 'amq.topic',queue='pasos',routing_key="#")



    channel.basic_consume(queue='pasos', on_message_callback=callback, auto_ack=True)
    channel.start_consuming()