const app = require('express')();
const http = require('http').Server(app);
const io = require('socket.io')(http);
var kafka = require('kafka-node'),
    Consumer = kafka.Consumer;


const client = new kafka.KafkaClient({kafkaHost: '192.168.0.144:9092'});
var scoreConsumer = new Consumer(
    client,
    [
        { topic: 'score', partition: 0 }
    ],
    {
        autoCommit: false
    }
);

scoreConsumer.on('message', function (message) {
    io.emit('score', {team: message});
});


io.on('connection', function(socket){
    console.log('Socket connected');
});

http.listen(9001, function(){
    console.log('listening on *:9001');
});