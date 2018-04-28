// var express = require('express');
// var app = express();
// var http = require('http').Server(app);
// var io = require('socket.io')(http);

// app.get('/',function(req, res) {
//     res.sendFile(__dirname + '/client/dashboard.html');
// });

// // middle ware to server static files
// app.use('/client', express.static(__dirname + '/client'));

// io.on('connection', function(socket){
//   console.log('a user connected');
// });

// http.listen(3000, function(){
//   console.log('listening on *:3000');
// });

var net = require('net');

var client = new net.Socket();
client.connect(9007, '127.0.0.1', function() {
	console.log('Connected');
});

client.on('data', function(data) {
	console.log('Received: ' + data);
});

client.on('close', function() {
	console.log('Connection closed');
});