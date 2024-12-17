import { Kafka } from "kafkajs";

const topic = "zap-topic";

const kafka = new Kafka({
  clientId: "outbox-processor-2",
  brokers: ["localhost:9092"],
});

async function main() {
  const consumer = kafka.consumer({ groupId: "main-worker-2" });

  await consumer.connect();
  await consumer.subscribe({ topic: topic, fromBeginning: true });
  await consumer.run({
    autoCommit: false,
    eachMessage: async ({ topic, partition, message }) => {
      console.log({
        partition,
        offset: message.offset,
        value: message.value?.toString(),
      });

      await new Promise((r) => setTimeout(r, 3000));

      const zapId = message.value?.toString();

      console.log("processing done");

      await consumer.commitOffsets([
        {
          topic: topic,
          partition: partition,
          offset: (parseInt(message.offset) + 1).toString(),
        },
      ]);
    },
  });
}
