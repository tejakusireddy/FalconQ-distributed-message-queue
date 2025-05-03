# 🦅 FalconQ – Distributed Message Queue with Priority-Based Messaging

![Go Version](https://img.shields.io/badge/Go-1.18+-brightgreen?logo=go)
![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)
![Status](https://img.shields.io/badge/Project-Active-brightgreen)
![PRs Welcome](https://img.shields.io/badge/PRs-welcome-blue.svg)
![Stars](https://img.shields.io/github/stars/tejakusireddy/FalconQ-distributed-message-queue?style=social)


FalconQ is a high-performance distributed message queue inspired by Apache Kafka, built in Go. It supports topic-based pub/sub, queue-based consumption, consumer offset tracking, **persistent storage**, and **priority-aware message delivery** — all from scratch.

> 🚀 Designed to showcase system design, distributed systems, and production-level Go backend skills. FAANG-ready.

---

## 📦 Features

- ✅ **Topic-based publish/subscribe** via REST API
- ✅ **High vs Low priority queueing** (handled during consumption)
- ✅ **Offset-tracked consumption** per consumer ID
- ✅ **Persistent Commit Log** using **BadgerDB** 💾
- ✅ **Batch consumption support**
- ✅ **Admin endpoints** for topic/partition insights
- ✅ Partitioned architecture *ready* for hashing logic (currently uses partition 0)
- 🔜 **Partitioning implementation** (Consistent Hashing)
- 🔜 Raft-based replication
- 🔜 Prometheus + Grafana observability
- 🔜 Chaos testing & 1.5M msg/min stress test

---

## 🚀 Getting Started

1.  **Prerequisites:**
    *   Go (version 1.18+ recommended) installed.
2.  **Clone the repository:**
    git clone https://github.com/tejakusireddy/FalconQ-distributed-message-queue.git
    cd falconq
3.  **Install dependencies:**
    go mod tidy
4.  **Run the broker:**
    # Make sure you are in the root directory of the project
    go run cmd/broker/main.go # Or wherever your main.go is located
    This will start the API server (usually on `localhost:8080`) and create a `falconq_data` directory in the same location for the BadgerDB storage.
5.  **Use `curl` (or other tools) to interact with the API** (see examples below).

---

## 🚦 API Endpoints

| Method | Route                                        | Description                                  |
| :----- :-------------------------------------------:-----------------------------------------------  |
| `POST` | `/topic/:topic/publish`                      |Publish a message(JSON: `message`, `priority`)|
| `GET`  | `/topic/:topic/consume?consumerID=X&batch=N` | Consume N messages (offset tracked)          |
| `GET`  | `/topic/:topic/peek?offset=X&batch=N`        | Peek messages starting at offset (no consumption)|
| `GET`  | `/topics`                                    | List all active topic names                      |
| `GET`  | `/topics/:topic/partitions`                  | View partition IDs for a specific topic          |

---

## 🧪 Example Usage (cURL)

# Publish messages to the 'orders' topic
curl -X POST http://localhost:8080/topic/orders/publish \
  -H "Content-Type: application/json" \
  -d '{"message": "🔥 Urgent refund request #RF001", "priority": "high"}'

curl -X POST http://localhost:8080/topic/orders/publish \
  -H "Content-Type: application/json" \
  -d '{"message": "🧊 Normal order placement #ORD001", "priority": "low"}'

curl -X POST http://localhost:8080/topic/orders/publish \
  -H "Content-Type: application/json" \
  -d '{"message": "🔥 Critical stock update #SKU001", "priority": "high"}'

# Consume messages for consumer 'worker1' (gets high priority first)
# First call:
curl "http://localhost:8080/topic/orders/consume?consumerID=worker1&batch=2"
# Example Response: [{"v":"🔥 Urgent refund request #RF001","p":"high","Offset":0},{"v":"🔥 Critical stock update #SKU001","p":"high","Offset":2}], nextOffset: 3

# Second call (will get low priority if available):
curl "http://localhost:8080/topic/orders/consume?consumerID=worker1&batch=2"
# Example Response: [{"v":"🧊 Normal order placement #ORD001","p":"low","Offset":1}], nextOffset: 2 (Note: actual offset depends on internal filtering)


# Peek messages starting from offset 0 (gets high priority first)
curl "http://localhost:8080/topic/orders/peek?offset=0&batch=5"

# View topics/partitions
curl http://localhost:8080/topics
# Example Response: {"topics":["orders"]}

curl http://localhost:8080/topics/orders/partitions
# Example Response: {"partitions":[{"id":0}],"topic":"orders"}



🧠 Architecture
mermaid

Architecture below illustrates how messages flow from REST → Broker → BadgerDB

graph TD
  subgraph User Facing API (Gin)
    Publish[POST /topic/:topic/publish]
    Peek[GET /topic/:topic/peek]
    Consume[GET /topic/:topic/consume]
    Topics[GET /topics]
    Partitions[GET /topics/:topic/partitions]
  end

  subgraph Broker Logic (Go)
    Broker -- Manages --> TopicsMap{Topics Map}

    TopicsMap --> TopicOrders(Topic: orders)
    TopicsMap --> TopicPayments(Topic: payments)

    TopicOrders -- Contains --> Partition0Orders(Partition 0)
    TopicPayments -- Contains --> Partition0Payments(Partition 0)

    Partition0Orders -- Manages --> OffsetsOrders[In-Memory Consumer Offsets]
    Partition0Orders -- Writes/Reads Log --> BadgerDB[(BadgerDB Persistent KV Store)]

    Partition0Payments -- Manages --> OffsetsPayments[In-Memory Consumer Offsets]
    Partition0Payments -- Writes/Reads Log --> BadgerDB
  end

  style BadgerDB fill:#f9f,stroke:#333,stroke-width:2px

  Publish --> Broker
  Peek --> Broker
  Consume --> Broker
  Topics --> Broker
  Partitions --> Broker


🛠️ Tech Stack
Language: Go
Framework: Gin (for REST API)
Data Store: BadgerDB (Persistent Key-Value Store / Commit Log)
Coordination: Planned Raft via hashicorp/raft
Observability: Planned Prometheus + Grafana


🛣️ Roadmap
✅ Phase 1 – In-memory priority queue concept, REST APIs
✅ Phase 2 – Add persistent commit log with BadgerDB
➡️ Phase 3 – Partitioning implementation (Consistent Hashing)
➡️ Phase 4 – Raft-based leader election and replication
➡️ Phase 5 – Kubernetes (EKS) deployment + Chaos Mesh
➡️ Phase 6 – Metrics, Tracing, Dashboard


👨‍💻 Author
Sai Teja Kusireddy
Snehith Kongara


🏁 License
MIT — feel free to fork, star, and build on top of it.


---







