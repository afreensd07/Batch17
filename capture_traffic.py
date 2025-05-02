from scapy.all import sniff, IP, TCP, UDP
from kafka import KafkaProducer
from kafka.errors import KafkaError
import time
import json

# Kafka Configuration
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    linger_ms=500,
    batch_size=32768
)

# Flow Tracking
flows = {}
FLOW_TIMEOUT = 300  # 5 minutes
CLEANUP_INTERVAL = 100  # Cleanup every 100 flows

def cleanup_old_flows():
    """Remove inactive flows to prevent memory leaks"""
    global flows
    current_time = time.time()
    expired = [
        flow_id for flow_id, flow in flows.items()
        if current_time - flow['last_time'] > FLOW_TIMEOUT
    ]
    for flow_id in expired:
        del flows[flow_id]

def calculate_flow_metrics(flow):
    """Calculate derived metrics matching training features"""
    duration = flow['end_time'] - flow['start_time']
    if duration > 0:
        flow['packet_rate'] = (flow['fwd_packets'] + flow['bwd_packets']) / duration
        flow['fwd_iat_avg'] = sum(flow['fwd_iat']) / len(flow['fwd_iat']) if flow['fwd_iat'] else 0
        flow['bwd_iat_avg'] = sum(flow['bwd_iat']) / len(flow['bwd_iat']) if flow['bwd_iat'] else 0
    else:
        flow['packet_rate'] = 0
        flow['fwd_iat_avg'] = 0
        flow['bwd_iat_avg'] = 0

def send_to_kafka(flow_data):
    """Send flow data with error handling"""
    try:
        future = producer.send(
            'flow_metrics',
            json.dumps(flow_data).encode('utf-8'))
        future.add_errback(lambda e: print(f"Kafka Error: {e}"))
    except KafkaError as e:
        print(f"Kafka Connection Error: {e}")

def process_packet(packet):
    global flows
    
    if not IP in packet:
        return

    ip = packet[IP]
    proto = ip.proto
    src_ip = ip.src
    dst_ip = ip.dst
    src_port = packet.sport if TCP in packet or UDP in packet else 0
    dst_port = packet.dport if TCP in packet or UDP in packet else 0

    # Sort IPs/ports to treat A→B and B→Aac
    sorted_ips = sorted([(src_ip, src_port), (dst_ip, dst_port)])
    flow_id = (
        sorted_ips[0][0], sorted_ips[0][1],
        sorted_ips[1][0], sorted_ips[1][1], 
        proto
    )
    current_time = time.time()

    # Initialize new flow
    if flow_id not in flows:
        flows[flow_id] = {
            'start_time': current_time,
            'end_time': current_time,
            'fwd_packets': 0,
            'bwd_packets': 0,
            'fwd_bytes': 0,
            'bwd_bytes': 0,
            'fwd_iat': [],
            'bwd_iat': [],
            'last_time': current_time,
            'last_fwd_time': None,
            'last_bwd_time': None
        }

    flow = flows[flow_id]
    flow['end_time'] = current_time
    flow['last_time'] = current_time

    # Determine direction
    direction = "fwd" if ((src_ip, src_port) == sorted_ips[0]) else "bwd"

    # Update counters
    flow[f'{direction}_packets'] += 1
    flow[f'{direction}_bytes'] += len(packet)
    
    # Calculate IAT
    if direction == "fwd":
        if flow['last_fwd_time'] is not None:
           iat = current_time - flow['last_fwd_time']
           flow['fwd_iat'].append(iat)
        flow['last_fwd_time'] = current_time
    else:
        if flow['last_bwd_time'] is not None:
            iat = current_time - flow['last_bwd_time']
            flow['bwd_iat'].append(iat)
        flow['last_bwd_time'] = current_time
    

    # Calculate derived metrics
    calculate_flow_metrics(flow)

    # Prepare Kafka message
    flow_data = {
        'flow_id': f"{sorted_ips[0][0]}:{sorted_ips[0][1]}-{sorted_ips[1][0]}:{sorted_ips[1][1]}-{proto}",
        'timestamp': current_time,
        'metrics': {
            'duration': flow['end_time'] - flow['start_time'],
            'fwd_packets': flow['fwd_packets'],
            'bwd_packets': flow['bwd_packets'],
            'fwd_bytes': flow['fwd_bytes'],
            'bwd_bytes': flow['bwd_bytes'],
            'fwd_iat_avg': flow['fwd_iat_avg'],
            'bwd_iat_avg': flow['bwd_iat_avg'],
            'packet_rate': flow['packet_rate']
        }
    }

    send_to_kafka(flow_data)

    # Periodic cleanup
    if len(flows) % CLEANUP_INTERVAL == 0:
        cleanup_old_flows()

def packet_handler(packet):
    try:
        process_packet(packet)
        print(f"Processed: {packet.summary()[:60]}...")
    except Exception as e:
        print(f"Packet Error: {str(e)}")

if __name__ == "__main__":
    try:
        print("Starting traffic capture on interface wlp0s20f3...")
        sniff(
            prn=packet_handler,
            iface="wlp0s20f3",
            store=False,
            filter="ip and not (udp port 1900 or udp port 53)"
        )
    except KeyboardInterrupt:
        print("\nStopping capture...")
        producer.flush()
        producer.close()