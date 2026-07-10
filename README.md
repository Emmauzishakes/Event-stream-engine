# A Streaming Engine

A high-performance, real-time WebRTC media and data broadcasting server built in Go. This microservice powers the core streaming infrastructure for the ChizziLive platform, managing ultra-low latency audio/video routing, active connection tracking, and real-time client engagement features.

## ✨ Features

*   **Pion WebRTC Integration:** Native, high-performance audio and video track routing.
*   **Production-Ready Trickle ICE:** Blazing-fast connection negotiation via asynchronous network route exchange over WebSockets.
*   **Dual-Track Streaming Engine:** Seamlessly binds synchronized audio and video streams inside a thread-safe `EventRoom` architecture.
*   **Real-Time Analytics & Stats:** Synchronous connection tracking offering global viewer counters and persistent room states.
*   **DDoS Protection & Data Batching:** Handles client-side aggregated data flushes (such as high-frequency engagement clicks) to protect memory loops and CPU cycles.
*   **Microservices Message Broker:** Fast JSON broadcasting for real-time live chat and room interactions.

---

## 🛠️ Tech Stack

*   **Language:** Go (1.21+)
*   **WebRTC Implementation:** [Pion WebRTC](github.com/pion/webrtc)
*   **WebSockets:** [Gorilla WebSocket](github.com/gorilla/websocket)

---

## 📂 Project Structure

```text
├── main.go          # Core engine architecture, WebSocket handlers & room routers
├── go.mod           # Go dependency specifications
├── go.sum           # Dependency checksum verifications
└── .gitignore       # Git exclusion rules