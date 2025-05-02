# Major_Project -17

## KAFKA-ENABLED REAL-TIME ANOMALY BASED INTRUSION DETECTION SYSTEM
The system design integrates Kafka as an anomaly-based intrusion detection real-time solution by using Apache Kafka data streaming and performing unsupervised anomaly detection during autoencoder warmup stage analysis with Kibana visualization functionalities. With Scapy the system can obtain real-time network traffic that Kafka producers convert into feature vectors. An autoencoder network learns traffic patterns through its encoder and decoder sections which use dropout regularization to create alerts when reconstruction errors exceed a set limit. The Kibana dashboards display anomaly detections using streaming operations which enables fast threat monitoring together with alert features for preventing damaging incidents.  

## 📌Objectives of the Project
1. Develop a real-time anomaly detection system using Kafka, autoencoder models, and Kibana for efficient network security monitoring.  
2. Ensure scalability and high-speed processing to handle large volumes of network traffic data.  
3. Provide anomaly detection and visualization of network anomalies for proactive threat mitigation.
   
## 🖇️Technology Stack
->Scapy

->TensorFlow

->Apache Kafka

->Kibana with Elasticsearch 

## 🗂️Dataset Used

~Scapy is a Python library for network packet manipulation, allowing users to sniff, craft, send, and analyze packets.

~It supports various protocols (TCP, UDP, ICMP, ARP) and is widely used in cybersecurity for penetration testing, intrusion detection, and network traffic analysis. 



## ⛓️How it works
**Data Ingestion & Preprocessing:**

   Captures live network traffic using Scapy and streams features to Kafka.

__Real-Time Data Streaming & Feature Processing__

   Uses Kafka to stream data to consumers for real-time preprocessing and feature extraction.

__Anomaly Detection with Autoencoder Model__

   Applies unsupervised learning to identify deviations from normal network behavior.

__Storage & Visualization__

   Streams anomalies to Kafka and visualizes them in real-time dashboards via Kibana.

__Alerting & Response Mechanism__

   Triggers alerts through Kibana based on detected anomaly scores.

## 📊Results

Kibana dashboard visualizing network traffic anomalies. It includes a time-series line graph, a heatmap, and a pie chart to differentiate between normal and anomalous network flows. A notable spike in anomalies is observed at 00:18:30, indicating a potential network security event.


