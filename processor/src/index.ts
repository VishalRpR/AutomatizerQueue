import { PrismaClient } from "@prisma/client";
import { Kafka } from "kafkajs";

const client = new PrismaClient();
const topic = "zap-topic";
const kafka = new Kafka({
  clientId: "outbox-processor",
  brokers: ["localhost:9092"],
});

async function main() {
  const producer = kafka.producer();
  await producer.connect();

  const zapruns = await client.zapRunOutbox.findMany({
    where: {},
    take: 10,
  });

  await producer.send({
    topic: topic,
    messages: zapruns.map((z) => {
      return {
        value: JSON.stringify({ zapRunId: z.zapRunId, stage: 0 }),
      };
    }),
  });

  await client.zapRunOutbox.deleteMany({
    where: {
      id: {
        in: zapruns.map((z) => z.id),
      },
    },
  });


  await new Promise(r=>setTimeout(r,3000));
}

main().catch(console.error);
