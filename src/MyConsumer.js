"use strict";

var kafka = require('kafka-node'),
    Consumer = kafka.Consumer,
    Client = kafka.KafkaClient;
var TopicsNotExistError = require('kafka-node').TopicsNotExistError;

class MyConsumer {
    constructor(host, topic, io, socket) {
        this.io = io;
        this.socket = socket;
        this.consumer = this.createClient();
        this.host = host;
        this.initEvents();
    }

    createClient() {
        return new Consumer(
            new kafka.KafkaClient({
                kafkaHost: this.host,
                connectRetryOptions: {
                    retries: 5,
                    factor: 3,
                    minTimeout: 1 * 1000,
                    maxTimeout: 60 * 1000,
                    randomize: true
                }
            }),
            [{topic: this.topic, partition: 0}],
            {autoCommit: true});
    }

    initEvents() {
        let self = this;
        this.consumer.on('message', function (message) {
            self.io.emit(self.socket, JSON.parse(message.value));
        });
        this.consumer.on('error', function (err) {
            console.log('Consumer error: ', err);
        });
    }
}

module.exports.MyConsumer = MyConsumer;