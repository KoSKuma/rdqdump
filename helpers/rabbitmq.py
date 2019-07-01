import pika


class RabbitMQ():
    def __init__(self, user, password, host, port, socket_timeout=60):
        self.user = user
        self.host = host
        self.port = port
        self.password = password
        self.socket_timeout = socket_timeout
        
        self.created_queues = []

    def connect_to_queue(self):
        credentials = pika.PlainCredentials(self.user, self.password)
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                host=self.host,
                port=int(self.port),
                credentials=credentials,
                socket_timeout=self.socket_timeout
            )
        )
        self.channel = self.connection.channel()

    def close(self):
        self.connection.close()

    def create_queue(self, queue_name, durable=True):
        if queue_name not in self.created_queues:
            self.channel.queue_declare(queue=queue_name, durable=durable)
            self.created_queues.append(queue_name)

    def publish_to_queue(self, queue_name, message):
        if self.connection.is_closed:
            self.connect_to_queue()

        self.channel.basic_publish(
            exchange='',
            routing_key=queue_name,
            body=message,
            properties=pika.BasicProperties(
                delivery_mode=2,  # make message persistent
            )
        )
