var balance = {};

var express = require('express');
var app = express();

app.get('/balance/:account', function (req, res) {
  var a = req.params.account;
  var data = { balance: balance[a] };
  res.json(data);
});
app.listen(3000);

var Kafka = require('node-rdkafka');

var consumer = new Kafka.KafkaConsumer({
    'group.id': 'kafkabank-nodejs',
    'metadata.broker.list': 'localhost:9092',
}, {});

consumer
  .on('ready', function() {
    consumer.subscribe(['transactions']);
    consumer.consume();
  })
  .on('data', function(data) {
    console.log(data.message.toString());
    var msg = data.message;
    if (!balance[msg.sender]) balance[msg.sender] = 0;
    if (!balance[msg.receiver]) balance[msg.receiver] = 0;
    balance[msg.sender] -= msg.amount;
    balance[msg.receiver] += msg.amount;
  });

consumer.connect();
