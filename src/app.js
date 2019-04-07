const app = require('express')();
const http = require('http').Server(app);
const io = require('socket.io')(http);
var kafka = require('kafka-node'),
    Consumer = kafka.Consumer;


const removeUserFromLobbyClient = new kafka.KafkaClient({kafkaHost: '192.168.0.144:9092'});
const joinLobbyClient = new kafka.KafkaClient({kafkaHost: '192.168.0.144:9092'});
const startGameClient = new kafka.KafkaClient({kafkaHost: '192.168.0.144:9092'});
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
    io.emit('remove-user-from-lobby', JSON.parse(message.value));
});



var joinLobbyConsumer = new Consumer(
    joinLobbyClient,
    [{ topic: 'join-lobby', partition: 0 }],
    {autoCommit: true}
);

joinLobbyConsumer.on('message', function (message) {
    io.emit('join-lobby', JSON.parse(message.value));
});

var startGameConsumer = new Consumer(
    startGameClient,
    [{topic: 'start-game-lobby', partition: 0}],
    {autoCommit: true}
);

startGameConsumer.on('message', function (message) {
    console.log(message);
   io.emit('start-game-lobby', JSON.parse(message.value));
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


joinLobbyConsumer.on('error', function (err) {
    console.log('Score consumer error: ', err);
});

app.get('/', function(req, res){
    res.send('<h1>Message server</h1>');
});

http.listen(9001, function(){
    console.log('listening on *:9001');
});