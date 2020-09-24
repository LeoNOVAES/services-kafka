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
        { value: message },
      ],
    });
}

const consumer = kafka.consumer({ groupId: 'antifraud'  });

// RODANDO CONSUMER PARA PEGAR OS EVENTOS DISPARADOS
const runConsumer = async () => {
    // Consuming
    await consumer.connect();
    await consumer.subscribe({ topic: 'events', fromBeginning: true });

    await consumer.run({
        eachMessage: async ({ topic, message }) => {
            const kafkaResponse = JSON.parse(message.value.toString());
            switch(kafkaResponse.typeEvent) {
                case 'create_order':
                    console.log(`topic -> ${topic}, message consumed -> , ${JSON.stringify(kafkaResponse)}`);
                    const isFraud = validatesIsFraud();
                    const payload = {
                        typeEvent: "is_fraud",
                        content: { isFraud }
                    };

                    runProducer(payload)
                    console.log("evento de resposta se e fraude disparado!")
                    
                    break;
                default:
                    console.log(`consume not know topic! -> ${topic}`);    
            }
        }
    });
}

runConsumer();

const validatesIsFraud = () => {
    console.log("validando se é fraude...");
    return false;
}

app.listen(3001,()=>{
    console.log("rodando na 3001")
});