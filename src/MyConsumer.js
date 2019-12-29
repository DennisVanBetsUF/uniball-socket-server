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
            this.createConsumerWithLastOffset();
        } else {
            this.consumer = this.createConsumer(null);
        }
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

    createConsumer(offset) {
        let self = this;
        this.client.topicExists([this.topic], error => {
            if (!error) {
                this.consumer = new Consumer(
                    this.client,
                    [{topic: self.topic, partition: 0, offset: offset !== null ? offset - 1 : null}],
                    {autoCommit: true, fromOffset: offset !== null});
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
            console.log('trying to post on socket: ' + self.socket + ' \n message: ' + message);
            try {
                self.io.emit(self.socket, JSON.parse(message.value));
            } catch (e) {
                console.log("Could not parse message.value: ", message);
            }
        });
        this.consumer.on('error', function (err) {
            console.log('Consumer error: ', err);
        });
    }

    createConsumerWithLastOffset() {
        let self = this;
        let offset = new Offset(this.client);
        offset.fetchLatestOffsets([this.topic], (error, offsets) => {
                if (error) return;
                let offset = offsets[self.topic][0]; //[0] = partition
                self.createConsumer(offset ? offset : 0);
        });
    }
}

module.exports.MyConsumer = MyConsumer;