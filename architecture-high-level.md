# Video Analytics Platform — High-Level Architecture

> Multi-Tenant | GPU-Accelerated | Kubernetes-Orchestrated

---

## 1. System Architecture — Block Diagram

```mermaid
%%{init: {'theme': 'base', 'themeVariables': { 'fontSize': '14px', 'lineColor': '#455A64' }}}%%
flowchart TB
    %% ── LAYER 0: CAMERAS ──
    CAM1["IP Camera 1\nRTSP/H.264"]:::cam
    CAM2["IP Camera 2\nRTSP/H.264"]:::cam
    CAM3["IP Camera N\nRTSP/H.264"]:::cam
    ONVIF["ONVIF\nDiscovery Service"]:::cam

    %% ── LAYER 1: CONTROL PLANE ──
    VST["VST\nCamera Management"]:::ctrl
    SDR["SDR\nSensor Distribution\nRouter"]:::ctrl
    WDM["WDM\nAuto-Scaler"]:::ctrl
    WDG["Watchdog\nReconciler\n(per pod)"]:::ctrl

    %% ── INFRA ──
    PG[("PostgreSQL\nCamera Registry\nSource of Truth")]:::infra
    RD[("Redis\nMessage Bus\nPub/Sub")]:::infra

    %% ── LAYER 2: DATA PLANE ──
    POD1["Pod 1 — DeepStream\nGPU 0 (L40S)\n50 streams"]:::gpu
    POD2["Pod 2 — DeepStream\nGPU 1 (L40S)\n50 streams"]:::gpu
    PODN["Pod N — DeepStream\nGPU N-1\n50 streams"]:::gpu

    %% ── LAYER 3: OUTPUT ──
    KAFKA["Apache Kafka\nAnalytics Events\n(JSON)"]:::output
    S3["S3 / MinIO\nRecordings (.mp4)\nSnapshots"]:::output
    RTMP["RTMP Server\nHLS / DASH\nLive Streams"]:::output
    PROM["Prometheus\nGPU/CPU Metrics\nPod Health"]:::output

    %% ── CONSUMERS ──
    DASH["Admin Dashboard\nReact / Vue\n15 Screens"]:::frontend
    ALERT["Alert Manager\nSMS · WhatsApp\nEmail · Webhook"]:::frontend
    PLAYER["Video Player\nWebRTC\nLive + Recorded"]:::frontend
    GRAFANA["Grafana\nMonitoring\nDashboards"]:::frontend

    %% ── K8s ──
    K8S["Kubernetes\nAPI Server"]:::k8s

    %% ═══ CONNECTIONS ═══

    %% Camera → Discovery
    CAM1 -->|"ONVIF Probe"| ONVIF
    CAM2 -->|"ONVIF Probe"| ONVIF
    CAM3 -->|"ONVIF Probe"| ONVIF

    %% Discovery → VST
    ONVIF -->|"Camera detected"| VST

    %% VST → DB + Redis
    VST -->|"INSERT camera\ntaken=FALSE"| PG
    VST -->|"Publish\ncamera_streaming"| RD

    %% Redis → SDR
    RD -->|"Event\nnotification"| SDR

    %% SDR → DB
    SDR -->|"Query pods\nAssign camera"| PG

    %% SDR → WDM (if scaling needed)
    SDR -->|"Capacity\nexceeded"| WDM

    %% WDM → K8s
    WDM -->|"Scale replicas"| K8S

    %% K8s → Pods
    K8S -->|"Provision\nnew GPU pod"| POD1
    K8S -->|"Provision\nnew GPU pod"| POD2
    K8S -->|"Provision\nnew GPU pod"| PODN

    %% Watchdog → DB + Pods
    WDG -->|"SELECT unclaimed\ncameras"| PG
    WDG -->|"POST /stream/add\nREST :9010"| POD1
    WDG -->|"POST /stream/add\nREST :9010"| POD2
    WDG -->|"POST /stream/add\nREST :9010"| PODN

    %% Cameras → Pods (RTSP)
    CAM1 -.->|"RTSP\nStream"| POD1
    CAM2 -.->|"RTSP\nStream"| POD1
    CAM3 -.->|"RTSP\nStream"| POD2

    %% Pods → Outputs
    POD1 -->|"Analytics\nJSON"| KAFKA
    POD2 -->|"Analytics\nJSON"| KAFKA
    PODN -->|"Analytics\nJSON"| KAFKA

    POD1 -->|"Recordings\n.mp4"| S3
    POD2 -->|"Recordings\n.mp4"| S3
    PODN -->|"Recordings\n.mp4"| S3

    POD1 -->|"Live\nFLV"| RTMP
    POD2 -->|"Live\nFLV"| RTMP
    PODN -->|"Live\nFLV"| RTMP

    POD1 -.->|"Metrics"| PROM
    POD2 -.->|"Metrics"| PROM
    PODN -.->|"Metrics"| PROM

    %% Outputs → Consumers
    KAFKA -->|"Events feed"| DASH
    KAFKA -->|"Alert trigger"| ALERT
    S3 -->|"Video clips"| PLAYER
    RTMP -->|"Live stream"| PLAYER
    PROM -->|"Metrics"| GRAFANA

    %% Dashboard → DB
    DASH -->|"REST API\nCRUD"| PG

    classDef cam fill:#0D47A1,stroke:#0D47A1,color:#fff,stroke-width:2px
    classDef ctrl fill:#1B5E20,stroke:#1B5E20,color:#fff,stroke-width:2px
    classDef infra fill:#4527A0,stroke:#4527A0,color:#fff,stroke-width:2px
    classDef gpu fill:#B71C1C,stroke:#B71C1C,color:#fff,stroke-width:2px
    classDef output fill:#E65100,stroke:#E65100,color:#fff,stroke-width:2px
    classDef frontend fill:#00695C,stroke:#00695C,color:#fff,stroke-width:2px
    classDef k8s fill:#1565C0,stroke:#1565C0,color:#fff,stroke-width:2px
```

---

## 2. DeepStream Pipeline — Common Buffer Block Diagram

```mermaid
%%{init: {'theme': 'base', 'themeVariables': { 'fontSize': '14px', 'lineColor': '#455A64' }}}%%
flowchart TD
    %% ── INPUT ──
    RTSP1["RTSP Source 1"]:::src
    RTSP2["RTSP Source 2"]:::src
    RTSPN["RTSP Source N\n(up to 50)"]:::src

    %% ── PIPELINE ──
    SRC["nvmultiurisrcbin\nMulti-Source Input"]:::pipeline
    MUX["nvstreammux\nbatch-size = 40"]:::pipeline
    INF["nvinfer\nYOLO Detection Model\n(GPU Inference)"]:::infer
    TEE["tee\nCOMMON BUFFER\nSingle inference → 3 outputs"]:::tee

    %% ── RECORDING BRANCH ──
    Q1["Queue\nRecording"]:::recBranch
    ENC1["H264 Encoder"]:::recBranch
    MUX1["MP4 Mux"]:::recBranch
    SINK1["File Sink"]:::recBranch
    S3["S3 / MinIO\nObject Storage"]:::storage

    %% ── STREAMING BRANCH ──
    Q2["Queue\nStreaming"]:::strBranch
    ENC2["H264 Encoder"]:::strBranch
    MUX2["FLV Mux"]:::strBranch
    SINK2["RTMP Sink"]:::strBranch
    RTMP_SVR["RTMP Server\nHLS / DASH"]:::storage

    %% ── ANALYTICS BRANCH ──
    Q3["Queue\nAnalytics"]:::anaBranch
    CONV["nvmsgconv\nMetadata → JSON"]:::anaBranch
    BROKER["nvmsgbroker"]:::anaBranch
    SINK3["Kafka Sink"]:::anaBranch
    KAFKA["Apache Kafka\nEvent Topics"]:::storage

    %% ═══ CONNECTIONS ═══
    RTSP1 -->|"H.264 stream"| SRC
    RTSP2 -->|"H.264 stream"| SRC
    RTSPN -->|"H.264 stream"| SRC

    SRC -->|"Decoded\nframes"| MUX
    MUX -->|"Batched\nframes"| INF
    INF -->|"Inferred\nframes + meta"| TEE

    TEE -->|"Branch 1"| Q1
    TEE -->|"Branch 2"| Q2
    TEE -->|"Branch 3"| Q3

    Q1 --> ENC1 --> MUX1 --> SINK1 -->|".mp4 files"| S3
    Q2 --> ENC2 --> MUX2 --> SINK2 -->|"FLV stream"| RTMP_SVR
    Q3 --> CONV --> BROKER --> SINK3 -->|"JSON events"| KAFKA

    classDef src fill:#0D47A1,stroke:#0D47A1,color:#fff,stroke-width:2px
    classDef pipeline fill:#1565C0,stroke:#1565C0,color:#fff,stroke-width:2px
    classDef infer fill:#B71C1C,stroke:#B71C1C,color:#fff,stroke-width:2px
    classDef tee fill:#E65100,stroke:#E65100,color:#fff,stroke-width:3px
    classDef recBranch fill:#2E7D32,stroke:#2E7D32,color:#fff,stroke-width:2px
    classDef strBranch fill:#6A1B9A,stroke:#6A1B9A,color:#fff,stroke-width:2px
    classDef anaBranch fill:#00695C,stroke:#00695C,color:#fff,stroke-width:2px
    classDef storage fill:#37474F,stroke:#37474F,color:#fff,stroke-width:2px
```

---

## 3. Frontend Screen Flow — Connected Navigation

```mermaid
%%{init: {'theme': 'base', 'themeVariables': { 'fontSize': '13px', 'lineColor': '#455A64' }}}%%
flowchart LR
    LOGIN["Login\nAuth / RBAC"]:::auth

    LOGIN -->|"Authenticate"| DASH["Dashboard\n(Screen 15)"]:::dash

    DASH -->|"Manage orgs"| ORG_D["Org Details\n(Screen 2)"]:::org

    ORG_D -->|"View sites"| SITE_D["Site Details\n(Screen 4)"]:::site

    SITE_D -->|"View cameras"| CAM_D["Camera Details\n(Screen 6)"]:::camera

    CAM_D -->|"Open stream"| LIVE["Live View +\nAnnotations\n(Screen 7)"]:::live
    CAM_D -->|"Configure AI"| ANA["Analytics\nConfig\n(Screen 8)"]:::analytics

    DASH -->|"Browse events"| EVT_L["Events Explorer\n(Screen 9)"]:::event
    EVT_L -->|"Select event"| EVT_D["Event Details\n(Screen 10)"]:::event

    DASH -->|"Manage alerts"| ALR_L["Alert Rules\n(Screen 11)"]:::alert
    ALR_L -->|"Create/Edit"| ALR_W["Alert Wizard\n(Screen 12)"]:::alert

    DASH -->|"Manage users"| USR_L["User List\n(Screen 13)"]:::user
    USR_L -->|"Add/Edit"| USR_D["User Details\n(Screen 14)"]:::user

    DASH -->|"View logs"| AUD["Audit Logs\n(Screen 16)"]:::system
    DASH -->|"Configure"| SYS["System Settings\n(Screen 17)"]:::system
    DASH -->|"View alerts"| NTF["Notifications\n(Screen 18)"]:::system

    classDef auth fill:#37474F,stroke:#263238,color:#fff,stroke-width:2px
    classDef dash fill:#1565C0,stroke:#0D47A1,color:#fff,stroke-width:3px
    classDef org fill:#6A1B9A,stroke:#4A148C,color:#fff,stroke-width:2px
    classDef site fill:#2E7D32,stroke:#1B5E20,color:#fff,stroke-width:2px
    classDef camera fill:#E65100,stroke:#BF360C,color:#fff,stroke-width:2px
    classDef live fill:#C62828,stroke:#B71C1C,color:#fff,stroke-width:2px
    classDef analytics fill:#AD1457,stroke:#880E4F,color:#fff,stroke-width:2px
    classDef event fill:#F57F17,stroke:#E65100,color:#000,stroke-width:2px
    classDef alert fill:#D32F2F,stroke:#B71C1C,color:#fff,stroke-width:2px
    classDef user fill:#00695C,stroke:#004D40,color:#fff,stroke-width:2px
    classDef system fill:#455A64,stroke:#37474F,color:#fff,stroke-width:2px
```

---

## 4. Auto-Scaling Flow — Connected Block Diagram

```mermaid
%%{init: {'theme': 'base', 'themeVariables': { 'fontSize': '14px', 'lineColor': '#455A64' }}}%%
flowchart TD
    NEW["100 New Cameras\nDetected via ONVIF"]:::trigger

    NEW -->|"ONVIF Probe"| VST["VST Service\nRegisters cameras"]:::ctrl
    VST -->|"INSERT\ntaken=FALSE"| DB[("PostgreSQL")]:::infra
    VST -->|"Publish event"| REDIS[("Redis")]:::infra

    REDIS -->|"camera_streaming"| SDR["SDR Service\nCalculates load"]:::ctrl
    SDR -->|"200 cameras\nneed 4 pods"| CHECK{"Current pods\nsufficient?"}:::decision

    CHECK -->|"YES\ncapacity available"| WDG_EXIST["Watchdog\nclaims on\nexisting pods"]:::ctrl
    CHECK -->|"NO\ncapacity exceeded"| WDM["WDM Auto-Scaler\nrequired = 4 pods\ncurrent = 2 pods"]:::ctrl

    WDM -->|"Scale to\n4 replicas"| K8S["Kubernetes\nAPI Server"]:::k8s
    K8S -->|"Provision\nGPU node"| GPU_OP["GPU Operator\nL40S driver"]:::k8s
    GPU_OP -->|"Node ready"| POD_NEW["New Pods 3 & 4\n0 cameras each"]:::gpu

    POD_NEW -->|"Watchdog\nstarts loop"| WDG_NEW["Watchdog\n(every 10s)"]:::ctrl
    WDG_NEW -->|"SELECT WHERE\ntaken=FALSE\nLIMIT 50"| DB
    DB -->|"Return 50\ncameras"| WDG_NEW
    WDG_NEW -->|"POST /stream/add"| DS["DeepStream\nPipeline"]:::gpu
    DS -->|"Stream added"| WDG_NEW
    WDG_NEW -->|"UPDATE\ntaken=TRUE"| DB

    WDG_EXIST -->|"Same loop"| DB

    WDG_NEW --> STEADY["Steady State\nPod 1: 50 (100%)\nPod 2: 50 (100%)\nPod 3: 50 (100%)\nPod 4: 50 (100%)\nTotal: 200/200"]:::success

    WDG_EXIST --> STEADY

    classDef trigger fill:#0D47A1,stroke:#0D47A1,color:#fff,stroke-width:2px
    classDef ctrl fill:#1B5E20,stroke:#1B5E20,color:#fff,stroke-width:2px
    classDef infra fill:#4527A0,stroke:#4527A0,color:#fff,stroke-width:2px
    classDef decision fill:#E65100,stroke:#E65100,color:#fff,stroke-width:3px
    classDef k8s fill:#1565C0,stroke:#1565C0,color:#fff,stroke-width:2px
    classDef gpu fill:#B71C1C,stroke:#B71C1C,color:#fff,stroke-width:2px
    classDef success fill:#2E7D32,stroke:#2E7D32,color:#fff,stroke-width:3px
```

---

## 5. End-to-End Data Flow

```mermaid
%%{init: {'theme': 'base', 'themeVariables': {'actorBkg': '#1a1a2e', 'actorTextColor': '#fff', 'actorBorder': '#455A64', 'signalColor': '#1565C0', 'signalTextColor': '#1a1a2e', 'noteBkgColor': '#FFF9C4', 'noteTextColor': '#000', 'noteBorderColor': '#F57F17', 'labelBoxBkgColor': '#455A64', 'labelTextColor': '#fff', 'loopTextColor': '#fff', 'fontSize': '13px'}}}%%
sequenceDiagram
    participant CAM as IP Camera
    participant VST as VST
    participant RD as Redis
    participant SDR as SDR
    participant DB as PostgreSQL
    participant K8 as K8s Scaler
    participant WD as Watchdog
    participant DS as DeepStream
    participant KF as Kafka
    participant S3 as S3 Storage
    participant FE as Frontend

    rect rgb(200, 230, 201)
        Note over CAM,FE: PHASE 1 — CAMERA REGISTRATION
        CAM->>VST: ONVIF Probe
        VST->>DB: INSERT camera (taken=FALSE)
        VST->>RD: Publish "camera_streaming"
        RD->>SDR: Event notification
        SDR->>DB: Query pods + assign camera
    end

    rect rgb(255, 224, 178)
        Note over CAM,FE: PHASE 2 — AUTO-SCALING (if needed)
        SDR->>K8: Request scale-up
        K8->>DS: Provision new GPU Pod
    end

    rect rgb(187, 222, 251)
        Note over CAM,FE: PHASE 3 — WATCHDOG RECONCILIATION
        loop Every 10 seconds
            WD->>DB: SELECT unclaimed cameras
            DB-->>WD: Return cameras
            WD->>DS: POST /api/v1/stream/add
            DS-->>WD: OK
            WD->>DB: UPDATE taken=TRUE
        end
    end

    rect rgb(252, 228, 236)
        Note over CAM,FE: PHASE 4 — CONTINUOUS STREAMING
        CAM->>DS: RTSP Stream (H.264, 30fps)
        DS->>DS: Decode → Infer → Common Buffer → Split
        par Recording
            DS->>S3: H264Enc → MP4Mux → .mp4
        and Streaming
            DS->>FE: H264Enc → FLVMux → RTMP/HLS
        and Analytics
            DS->>KF: nvmsgconv → Kafka (JSON)
        end
        KF->>FE: Live event feed
        S3->>FE: Video clips on demand
    end
```

---

## 6. Layer Summary

| Layer | Components | Input | Output |
|-------|------------|-------|--------|
| **Layer 0 — Ingestion** | IP Cameras, ONVIF Discovery | Camera network | RTSP streams, camera registration |
| **Layer 1 — Control Plane** | VST, SDR, Watchdog, WDM | Registered cameras | Stream assignments, pod scaling |
| **Layer 2 — Data Plane** | DeepStream Pods (GPU) | RTSP streams | Recordings, live streams, events |
| **Layer 3 — Output** | Kafka, S3, RTMP, Prometheus | Pipeline outputs | Consumer-ready data |
| **Frontend** | 15 Admin Screens | REST API / WebSocket | User interface |

## 7. Key Design Principles

| Principle | How It Works |
|-----------|-------------|
| **Common Buffer** | Single GPU inference pass splits into 3 outputs via `tee` — no redundant processing |
| **Decentralized Reconciliation** | Each pod runs its own watchdog to independently claim cameras |
| **Database as Source of Truth** | `taken` flag in PostgreSQL prevents double-assignment across pods |
| **K8s GPU Orchestration** | GPU Operator + WDM_MAX_REPLICAS handles hardware scaling |
| **Event + Polling Hybrid** | Redis for immediate events, watchdog for periodic reconciliation |
| **Location Awareness** | Cameras assigned to geographically closest pods |
| **Self-Healing** | Components fail independently; system self-recovers via watchdog loop |

## 8. Scaling Parameters

| Parameter | Value | Purpose |
|-----------|-------|---------|
| L40S Capacity | 50 streams/GPU | Max RTSP streams per L40S pod |
| T4 Capacity | 20 streams/GPU | Max RTSP streams per T4 pod |
| A100 Capacity | 80 streams/GPU | Max RTSP streams per A100 pod |
| Watchdog Interval | 10 seconds | Reconciliation loop frequency |
| Batch Size | 40 | DeepStream muxer batch size |
| WDM_MAX_REPLICAS | total_gpus - 1 | Maximum pod replicas |
