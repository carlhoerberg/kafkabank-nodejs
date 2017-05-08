var balance = {};

var fs = require('fs');
var Kafka = require('node-rdkafka');

var kafkaConf = {
  'group.id': 'kafkabank-nodejs',
  'metadata.broker.list': 'localhost:9092',
  'socket.keepalive.enable': true,
  'enable.auto.commit': false,
};

var topics = ["transactions"];
if (process.env.CLOUDKARAFKA_TOPIC_PREFIX) {
  topics = topics.map(function(t) { return process.env.CLOUDKARAFKA_TOPIC_PREFIX + t; });
  fs.writeFileSync("/tmp/kafka.ca", process.env.CLOUDKARAFKA_CA);
  fs.writeFileSync("/tmp/kafka.crt", process.env.CLOUDKARAFKA_CERT);
  fs.writeFileSync("/tmp/kafka.key", process.env.CLOUDKARAFKA_PRIVATE_KEY);
  kafkaConf["ssl.ca.location"] = "/tmp/kafka.ca";
  kafkaConf["ssl.certificate.location"] = "/tmp/kafka.crt";
  kafkaConf["ssl.key.location"] = "/tmp/kafka.key";
  kafkaConf["security.protocol"] = "ssl";
}

var consumer = new Kafka.KafkaConsumer(kafkaConf, {
  'auto.offset.reset': 'beginning'
});

consumer
  .on('ready', function() {
    consumer.subscribe(topics);
    consumer.consume();
  })
  .on('error', function(err) {
    console.log(err);
    process.exit(1);
  })
  .on('data', function(data) {
    var msg = JSON.parse(data.value);
    if (!balance[msg.sender]) balance[msg.sender] = 0;
    if (!balance[msg.receiver]) balance[msg.receiver] = 0;
    balance[msg.sender] -= msg.amount;
    balance[msg.receiver] += msg.amount;
    console.log(balance);
  });

consumer.connect();

var express = require('express');
var app = express();

app.get('/balance/:account', function (req, res) {
  var a = req.params.account;
  var data = { balance: balance[a] };
  res.json(data);
});
app.listen(process.env.PORT || 3000);

process.on('SIGINT', function() {
  console.log("Caught interrupt signal");

  consumer.disconnect();
  process.exit();
});
