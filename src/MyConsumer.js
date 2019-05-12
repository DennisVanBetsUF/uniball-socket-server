var kafka = require('kafka-node'),
    Consumer = kafka.Consumer;
Client = kafka.KafkaClient;

class MyConsumer {
    consumer;
    io;
    socket;

    constructor(host, topic, io, socket) {
        this.io = io;
        this.socket = socket;
        this.consumer = new Consumer(
            new kafka.KafkaClient({kafkaHost: host}),
            [{topic: topic, partition: 0}],
            {autoCommit: true});

        this.initEvents();
    }

    initEvents() {
        let self = this;
        this.consumer.on('message', function (message) {
            self.io.emit(self.socket,  JSON.parse(message.value));
        });
        this.consumer.on('error', function (err) {
            console.log('Consumer error: ', err);
        });
    }
}

module.exports.MyConsumer = MyConsumer;