var express = require('express');
var app = express();
var serv = require('http').Server(app);

app.get('/das',function(req, res) {
    res.sendFile(__dirname + '/client/dashboard.html');
});

serv.listen(3500);

var io = require('socket.io')(serv,{});

io.sockets.on('connection', function(socket) {
  	socket.on('dashboard_on', function() {
	  	console.log("dashboard is on");

	  	var net = require('net');
	  	var client = new net.Socket();
	  	client.connect(9007, '127.0.0.1', function() {
	  		console.log('Connected');
	  	});
	  	client.on('data', function(data) {
			console.log('Received: ' + data);
			socket.emit('alert', "Alert: " + data);
		});
		client.on('close', function() {
			console.log('Connection closed');
		});
  	});
});

app.use('/client', express.static(__dirname + '/client'));
