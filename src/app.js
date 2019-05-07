const app = require('express')();
const http = require('http').Server(app);
const io = require('socket.io')(http);
var kafka = require('kafka-node'),
    Consumer = kafka.Consumer;

let kafkaUrl = '192.168.0.144:9092';
if (process.argv[2] === 'prod') {
    kafkaUrl = 'http://kafka:9092';
}


const removeUserFromLobbyClient = new kafka.KafkaClient({kafkaHost: kafkaUrl});
const joinLobbyClient = new kafka.KafkaClient({kafkaHost: kafkaUrl});
const startGameClient = new kafka.KafkaClient({kafkaHost: kafkaUrl});
const teamCreatedClient = new kafka.KafkaClient({kafkaHost: kafkaUrl});
const scoreClient = new kafka.KafkaClient({kafkaHost: kafkaUrl});
const scoreEventClient = new kafka.KafkaClient({kafkaHost: kafkaUrl});

var scoreEventConsumer = new Consumer(
    scoreEventClient,
    [{ topic: 'score-event', partition: 0 }],
    {autoCommit: true}
);

scoreEventConsumer.on('message', function (message) {
    io.emit('score-event',  JSON.parse(message.value));
});

var scoreConsumer = new Consumer(
    scoreClient,
    [{ topic: 'score', partition: 0 }],
    {autoCommit: true}
);

scoreConsumer.on('message', function (message) {
    io.emit('score', {team: JSON.parse(message.value)});
});

var teamCreatedConsumer = new Consumer(
    teamCreatedClient,
    [{ topic: 'team-created', partition: 0 }],
    {autoCommit: true}
);

teamCreatedConsumer.on('message', function(message) {
    console.log(message);
   io.emit('team-created', JSON.parse(message.value));
});

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