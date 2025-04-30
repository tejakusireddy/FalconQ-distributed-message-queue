# distributed-message-queue
A fault-tolerant distributed message queue system inspired by Kafka, with priority messaging, replication, and real-time monitoring.
# 🦅 FalconQ – Distributed Message Queue with Priority-Based Messaging

FalconQ is a high-performance distributed message queue inspired by Apache Kafka, built in Go. It supports topic-based pub/sub, queue-based consumption, consumer offset tracking, and **priority-aware message delivery** — all from scratch.

> 🚀 Designed to showcase system design, distributed systems, and production-level Go backend skills. FAANG-ready. 

---

## 📦 Features

- ✅ **Topic-based publish/subscribe** via REST API
- ✅ **High vs Low priority queueing**
- ✅ **Offset-tracked consumption** per consumer ID
- ✅ **Partitioned architecture** (starting with partition 0)
- ✅ **In-memory data store** (RocksDB coming soon)
- ✅ **Batch consumption support**
- ✅ **Admin endpoints** for topic/partition insights
- 🔜 RocksDB persistence
- 🔜 Raft-based replication
- 🔜 Prometheus + Grafana observability
- 🔜 Chaos testing & 1.5M msg/min stress test

---

## 🚦 API Endpoints

| Method | Route | Description |
|--------|-------|-------------|
| `POST` | `/topic/:topic/publish` | Publish a message (JSON: `message`, `priority`) |
| `GET`  | `/topic/:topic/consume?consumerID=X&batch=N` | Consume N messages (offset tracked) |
| `GET`  | `/topic/:topic/peek?offset=X&batch=N` | Peek messages without consuming |
| `GET`  | `/topics` | List all topics |
| `GET`  | `/topics/:topic/partitions` | View partition stats (message count, etc.) |

---

## 🧪 Example Usage (cURL)


# Publish messages
curl -X POST http://localhost:8080/topic/orders/publish \
  -H "Content-Type: application/json" \
  -d '{"message": "🔥 Urgent refund", "priority": "high"}'

curl -X POST http://localhost:8080/topic/orders/publish \
  -H "Content-Type: application/json" \
  -d '{"message": "🧊 Normal order", "priority": "low"}'

# Consume
curl "http://localhost:8080/topic/orders/consume?consumerID=worker1&batch=1"

# Peek
curl "http://localhost:8080/topic/orders/peek?offset=0&batch=5"

# View topics/partitions
curl http://localhost:8080/topics
curl http://localhost:8080/topics/orders/partitions


🧠 Architecture
mermaid

graph TD

  Broker --> TopicOrders[Topic: orders]
  Broker --> TopicPayments[Topic: payments]

  TopicOrders --> Partition0Orders[Partition 0]
  TopicPayments --> Partition0Payments[Partition 0]

  Partition0Orders --> HighQueue1[High Priority Queue]
  Partition0Orders --> LowQueue1[Low Priority Queue]
  Partition0Orders --> Offsets1[Consumer Offsets]

  Partition0Payments --> HighQueue2[High Priority Queue]
  Partition0Payments --> LowQueue2[Low Priority Queue]
  Partition0Payments --> Offsets2[Consumer Offsets]

  subgraph API Endpoints
    Publish[POST /topic/:topic/publish]
    Peek[GET /topic/:topic/peek]
    Consume[GET /topic/:topic/consume]
    Topics[GET /topics]
    Partitions[GET /topics/:topic/partitions]
  end

  Publish --> Broker
  Peek --> Broker
  Consume --> Broker
  Topics --> Broker
  Partitions --> Broker

🛠️ Tech Stack
Language: Go

Framework: Gin (for REST API)

Data Store: In-memory (Phase 1), RocksDB (Phase 2)

Coordination: Planned Raft via hashicorp/raft

Observability: Planned Prometheus + Grafana

🛣️ Roadmap
 Phase 1 – In-memory priority queue, REST APIs

 Phase 2 – Add persistent log with RocksDB

 Phase 3 – Partitioning with consistent hashing

 Phase 4 – Raft-based leader election and replication

 Phase 5 – Kubernetes (EKS) deployment + Chaos Mesh

 Phase 6 – Metrics, Tracing, Dashboard

👨‍💻 Author
Sai Teja Kusireddy

🏁 License
MIT — feel free to fork, star, and build on top of it.

---







