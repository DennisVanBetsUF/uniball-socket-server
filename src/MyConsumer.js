"use strict";

var kafka = require('kafka-node'),
    Consumer = kafka.Consumer,
    Client = kafka.KafkaClient;

class MyConsumer {
    constructor(host, topic, io, socket) {
        this.io = io;
        this.socket = socket;
        this.host = host;
        this.topic = topic;
        this.client = this.createClient();
        this.consumer = this.createConsumer();
    }

    createClient() {
        return new Client({
            kafkaHost: this.host,
            connectRetryOptions: {
                retries: 5,
                factor: 3,
                minTimeout: 10 * 1000,
                maxTimeout: 60 * 1000,
                randomize: true
            }
        });
    }

    createConsumer() {
        let self = this;
        this.client.topicExists([this.topic], error => {
            if (!error) {
                this.consumer = new Consumer(
                    this.client,
                    [{topic: self.topic, partition: 0}],
                    {autoCommit: true});
                self.initEvents();
            } else {
                console.log(error);
                setTimeout(() => {
                    console.log("Trying to recreating consumer for topic: " + self.topic);
                    self.createConsumer();
                }, 30000);
            }
        })
    }


    initEvents() {
        let self = this;
        this.consumer.on('message', function (message) {
            console.log('posting on socket: ' + self.socket + ' \n message: ' + message);
            self.io.emit(self.socket, JSON.parse(message.value));
        });
        this.consumer.on('error', function (err) {
            console.log('Consumer error: ', err);
        });
    }
}

module.exports.MyConsumer = MyConsumer;