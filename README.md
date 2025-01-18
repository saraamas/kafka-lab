# Real-Time News Topic Detection and Visualization

## **System Setup**

1. First of all initialize Kafka and zookeeper services using docker-compose :

`````````bash
docker-compose up -d --build
``````````````````

2. Then run the producer with :
`````````bash
python producer.py
``````````````````

3. And finally the consumer with :
`````````bash
streamlit run consumer.py

``````````````````
