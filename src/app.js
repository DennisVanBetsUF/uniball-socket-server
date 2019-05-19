"use strict";
var kafka = require('kafka-node'),
    Consumer = kafka.Consumer;
const MyConsumer= require("./MyConsumer").MyConsumer;

const app = require('express')();
const http = require('http').Server(app);
const io = require('socket.io')(http);


let kafkaUrl = '192.168.0.144:9092';
if (process.argv[2] === 'prod') {
    kafkaUrl = 'kafka:9092';
}

var ioConsumers = [];

const consumers = [
    {topic: 'score-event', socket: 'score-event'},
    {topic: 'score', socket: 'score'},
    {topic: 'team-created', socket: 'team-created'},
    {topic: 'remove-user-from-lobby', socket: 'remove-user-from-lobby'},
    {topic: 'join-lobby', socket: 'join-lobby'},
    {topic: 'start-game-lobby', socket: 'start-game-lobby'}
];

consumers.forEach(c => {
    ioConsumers.push({id: c.topic + '->' + c.socket, consumer: new MyConsumer(kafkaUrl, c.topic, io, c.socket)});
});

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
});

app.get('/', function(req, res){
    res.send('<h1>Message server</h1>');
});

http.listen(9001, function(){
    console.log('listening on *:9001');
});