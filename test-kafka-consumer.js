const { Kafka } = require('kafkajs');
const express = require('express');
const http = require('http');
const socketIo = require('socket.io');
const port = 3030;

const app = express();
const server = http.createServer(app);
const io = socketIo(server, {
  cors: {
    origin: '*',
  }
});

const kafka = new Kafka({
  clientId: 'test-client',
  brokers: ['183.82.105.140:9092']
});

const consumer = kafka.consumer({ groupId: 'rad-dashbord-orders' });

const run = async () => {
  await consumer.connect();
  // Subscribe to multiple topics
  await consumer.subscribe({ topics: ['radiology-dashbord-count', 'radiology-speciality-count', 'lab-dashbord-count', 'lab-speciality-Count'], fromBeginning: true });

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      try {
        const value = message.value.toString();
        console.log(`Received message from ${topic}: ${value}`);
        io.emit('event', { topic, value });
      } catch (error) {
        console.error('Error processing message:', error);
      }
    },
  });
};

run().catch(console.error);

io.on('connection', (client) => {
  console.log('Client connected:', client.id);

  client.on('disconnect', () => {
    console.log('Client disconnected:', client.id);
  });
});

server.listen(port, () => {
  console.log(`Server listening on port ${port}`);
});


