"use strict";
const MyConsumer= require("./MyConsumer").MyConsumer;

const app = require('express')();
const http = require('http').Server(app);
const io = require('socket.io')(http, {});

let kafkaUrl = 'localhost:9092';
let kafkaConnectDelay = 0;
if (process.argv[2] === 'prod') {
    kafkaUrl = 'kafka:19092';
    kafkaConnectDelay = 30000;
}

var ioConsumers = [];

const consumers = [
    {topic: 'score-event', socket: 'score-event'},
    {topic: 'score', socket: 'score'},
    {topic: 'team-created', socket: 'team-created'},
    {topic: 'join-lobby', socket: 'join-lobby'},
    {topic: 'remove-user-from-lobby', socket: 'remove-user-from-lobby'},
    {topic: 'start-game-lobby', socket: 'start-game-lobby'}
];
console.log('waiting for kafka...');

setTimeout(() => {
    console.log("Connecting to consumers...");
    consumers.forEach(c => {
        ioConsumers.push({id: c.topic + '->' + c.socket, consumer: new MyConsumer(kafkaUrl, c.topic, io, c.socket, false)});
        console.log('Created consumer: ', ioConsumers[ioConsumers.length-1].id);
    });
}, kafkaConnectDelay);

io.on('connection', function(socket){
    socket.on('user-selected-team', data => {
        socket.broadcast.emit('user-selected-team-broadcast', data);
    });
    socket.on('user-selected-role', data => {
        socket.broadcast.emit('user-selected-role-broadcast', data);
    });
    socket.on('reset-team-selector-of-users', data => {
        socket.broadcast.emit('reset-team-selector-of-users-broadcast', data);
    });
    socket.on('assign-team-to-users', data => {
        socket.broadcast.emit('assign-team-to-users-broadcast', data);
    });
    socket.on('subscribe-to-game-created-events', data => {
       let startGameOffsetConsumer = new MyConsumer(kafkaUrl, 'start-game-lobby', socket, 'subscribe-to-game-created-events', true);
    });
});

app.get('/', function(req, res) {
    res.send('<h1>Message server</h1>');
});

http.listen(9001, function(){
    console.log('listening on *:9001');
});