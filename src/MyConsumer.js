"use strict";

var kafka = require('kafka-node'),
    Consumer = kafka.Consumer,
    Client = kafka.KafkaClient,
    Offset = kafka.Offset;

class MyConsumer {
    constructor(host, topic, io, socket, withFirstOffsetFetch = false) {
        this.io = io;
        this.socket = socket;
        this.host = host;
        this.topic = topic;
        this.client = this.createClient();
        if (withFirstOffsetFetch === true) {
            this.fetchOffset();
        }
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
                    {autoCommit: true, fromOffset: true});
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

    fetchOffset() {
        let self = this;
        let offset = new Offset(this.client);
        offset.fetch([
            {topic: this.topic, partition: 0, time: -1, maxNum: 1} //fetch latest available (time: -1)
            ], (error, data) => {
                self.consumer.
                console.log('fetching offset fot topic: ', self.topic, error, data);
                offset.get()
        });
    }
}

module.exports.MyConsumer = MyConsumer;