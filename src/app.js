const app = require('express')();
const http = require('http').Server(app);
const io = require('socket.io')(http);
var kafka = require('kafka-node'),
    Consumer = kafka.Consumer;


const removeUserFromLobbyClient = new kafka.KafkaClient({kafkaHost: '192.168.0.144:9092'});
const joinLobbyClient = new kafka.KafkaClient({kafkaHost: '192.168.0.144:9092'});
// var scoreConsumer = new Consumer(
//     client,
//     [{ topic: 'score', partition: 0 }],
//     {autoCommit: false}
// );
//
// scoreConsumer.on('ready', function () {
//     console.log('Score consumer ready');
// });
//
// scoreConsumer.on('message', function (message) {
//     io.emit('score', {team: message});
// });

var removeUserFromLobbyConsumer = new Consumer(
    removeUserFromLobbyClient,
    [{ topic: 'remove-user-from-lobby', partition: 0 }],
    {autoCommit: true}
);

removeUserFromLobbyConsumer.on('message', function (message) {
    console.log('removing user from lobby:', message);
    io.emit('remove-user-from-lobby', message);
});

var joinLobbyConsumer = new Consumer(
    joinLobbyClient,
    [{ topic: 'join-lobby', partition: 0 }],
    {autoCommit: true}
);

joinLobbyConsumer.on('message', function (message) {
    console.log('joining lobby:', message);
    io.emit('join-lobby', message);
});

joinLobbyConsumer.on('error', function (err) {
    console.log('Score consumer error: ', err);
});


io.on('connection', function(socket){
    console.log('Socket connected');
    socket.on('user-selected-team', data => {
        console.log('selected team', data);
        socket.broadcast.emit('user-selected-team-broadcast', data);
    });
    socket.on('user-selected-role', data => {
        socket.broadcast.emit('user-selected-role-broadcast', data);
    });
});

app.get('/', function(req, res){
    res.send('<h1>Message server</h1>');
});

http.listen(9001, function(){
    console.log('listening on *:9001');
});