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

const consumer = kafka.consumer({ groupId: 'pagamentos'  });

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
            
                    const payload = {
                        typeEvent: "verify_fraud",
                        content: { 
                            payment: "dados de pagamento" 
                        }
                    };

                    runProducer(payload).then(() => {
                        console.log("evento se e fraude disparado!")
                    })
                        
                    break;

                case 'is_fraud':
                    console.log(`topic -> ${topic}, message consumed -> , ${JSON.stringify(kafkaResponse)}`);
                    if(message.content.isFraud) {
                        console.log("e fraud o fluxo vai ser paraado por aqui...")    
                        return;
                    }

                    handlerPayment();
                    break;    
                default:
                    console.log(`consume not know topic! -> ${topic}`);    
            }
        }
    });
}

runConsumer();

const handlerPayment = () => {
    console.log("realizando pagamento ...");
    const payload = {
        typeEvent: "completed_order"
    }

    runProducer(payload)
    console.log("evento d finalizacao de pedido disparado!")
}

app.listen(3000,()=>{
    console.log("rodando na 3000")
});