const express = require("express");
const { Kafka } = require('kafkajs');

const app = express();
app.use(express.json());

// CONFIGURAÇÃO DO KAFKA
const kafka = new Kafka({
    clientId: 'pb-notifications',
    brokers: ['localhost:9092']
});

const producer = kafka.producer();

// PARA DISPARAR ALGUMA MENSAGEM
const runProducer = async (message) => {
    // Producing
    await producer.connect()
    await producer.send({
      topic: 'events',
      messages: [
        { value: message.toString() },
      ],
    });
}

const consumer = kafka.consumer({ groupId: 'pedidos'  });

// RODANDO CONSUMER PARA PEGAR OS EVENTOS DISPARADOS
const runConsumer = async () => {
    // Consuming
    await consumer.connect();
    await consumer.subscribe({ topic: 'events', fromBeginning: true });

    await consumer.run({
        eachMessage: async ({ topic, message }) => {
            const kafkaResponse = JSON.parse(message.value.toString());
            console.log("\n\n\n\n", kafkaResponse, "\n\n\n\n")
            switch(kafkaResponse.eventType) {
                case 'completed_order':
                    console.log(`topic -> ${topic}, message consumed -> , ${JSON.stringify(kafkaResponse)}`);
                    finish()
                    break;
                default:
                    console.log(`consume not know topic! -> ${topic}`);    
            }
        }
    });
}

runConsumer();

app.post('/order', (req, res) => {
    console.log("criando pedido ... ")

    const payload = {
        typeEvent:'create_order',
        content: {
            item:"televisao",
            price: 1000
        }
    }

    runProducer(payload);
    return res.send("pedido criado");
});

const finish = () => {
    console.log("pedido concluido separando mercadorias e enviando...");
}

app.listen(3002,()=>{
    console.log("rodando na 3002")
});