## 🏭 IIoT Machine Telemetry & Control Engine (Cloud Demo)

A cloud-native, end-to-end Industrial IoT (IIoT) data streaming pipeline. This system processes telemetry across **5 distinct press machines**, enforces dynamic operational thresholds using a split-schema GitOps architecture, reroutes structural anomalies to a Dead Letter Queue (DLQ), and exposes interactive web dashboards.

### 🏗️ Cloud Infrastructure Architecture

*   **Compute (Render):** Hosts two interactive Python services (Producer Controller & Consumer Dashboard).
*   **Data & Schema Registry (Aiven):** Managed cloud Apache Kafka 4.2 broker and Karapace Registry (mTLS protected).
*   **Automation (GitHub Actions):** Continuous Integration/Deployment loop for serverless schema and rule provisioning.

---

### 🚀 Live Demo Operation Steps

#### 1. Generate Telemetry (The Producer Panel)
*   Open your public Producer URL: `https://onrender.com`
*   **Mode A (Continuous Stream):** Click **"Start 5-Machine Stream Loop"** to fire randomized fluctuating metrics (`Press-01` to `Press-05`) across the cloud every 1.5 seconds.
*   **Mode B (Manual Override):** Select a specific Asset ID from the dropdown, input custom values (e.g., `75.0°C`, `150 PSI`), and click **"Fire Targeted Payload Packet"** to simulate an on-the-fly factory floor emergency.

#### 2. Monitor Pipelines (The Consumer Ingestion Dashboard)
*   Open your public Ingestion URL: `https://onrender.com`
*   Watch real-time data flow with short-fingerprint configuration hashes (`v.4c8227fc`).
*   **Auto-Refresh Toggle:** Click the **`🔄 Auto-Refresh: ON/OFF`** button in the upper right corner to pause the real-time layout stream and safely audit specific data entries without losing your place.
*   **Smart Logging & Alarms:** Normal states log as green. If an asset breaches limits, a high-visibility red `🚨 ALARM` triggers—highlighting the exact violation while preserving normal metrics context in brackets.

---

### ⚙️ How to Evolve Operational Rules (GitOps Loop)

To change operational safe limits (e.g., raising max temperature from `40°C` to `60°C`) dynamically without restarting any cloud infrastructure or modifying backend code:

1.  Open your local repository and edit the **`machine-control.json`** parameters file:
    ```json
    {
      "version": 4,
      "rules": {
        "max_safe_temp": 60.0,
        "max_safe_pressure": 140.0
      }
    }
    ```
2.  Commit and push the file change directly to your main branch layout:
    ```bash
    git add machine-control.json
    git commit -m "ci: updated safe operational temperature thresholds for factory line"
    git push origin main
    ```
3.  **The Automation Loop:** **GitHub Actions** will instantly wake up, validate the JSON string format, and push the rules up to **Aiven Karapace Registry**.
4.  **The Result:** Within 10 seconds, the running **Render Ingestion Engine** will pull the updated thumbprint hash, adjust its boundaries live, and update the UI thresholds automatically!

---

### 🔖 Code Milestones (Reference Safety Nets)

*   `v1.0.0-local`: Permanent snapshot of the fully functioning local Docker Compose + Apicurio ecosystem.
*   `v2.0.0-cloud`: Pristine snapshot of the cloud-native Aiven + Render deployment with interactive Flask web views.
