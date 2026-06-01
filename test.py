from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'option.metrics.realtime',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    auto_offset_reset='latest',
)

print("等待消息...")
for msg in consumer:
    print(f"收到: {msg.value.get('symbol')}")