# Video Analytics Platform — Detailed Component Architecture

> Complete Screen Specifications | Backend Pipeline | Auto-Scaling Flows

---

## Table of Contents

1. [Frontend — Screen Architecture](#1-frontend--screen-architecture)
2. [Backend — Layer 0: Ingestion](#2-backend--layer-0-ingestion)
3. [Backend — Layer 1: Control Plane](#3-backend--layer-1-control-plane)
4. [Backend — Layer 2: Data Plane](#4-backend--layer-2-data-plane-deepstream-pipeline)
5. [Backend — Layer 3: Output](#5-backend--layer-3-output)
6. [Auto-Scaling Flow (100 → 200 cameras)](#6-auto-scaling-flow)
7. [Watchdog Reconciliation Loop](#7-watchdog-reconciliation-loop)
8. [End-to-End Data Flow](#8-end-to-end-data-flow)
---

## 1. Frontend — Screen Architecture

### Full Screen Flow — Connected Block Diagram

```mermaid
%%{init: {'theme': 'base', 'themeVariables': { 'fontSize': '13px', 'lineColor': '#455A64' }}}%%
flowchart TD
    LOGIN["Login / Auth\nRBAC"]:::auth

    LOGIN -->|"Authenticate"| DASH

    subgraph DASH_GROUP["Dashboard Hub"]
        DASH["Screen 15: Dashboard\nMetrics · Charts · Map\nLive Feed · System Health"]:::dash
    end

    %% ── ORG FLOW ──
    DASH -->|"Manage org"| ORG_D
    subgraph ORG_FLOW["Organization"]
        ORG_D["Screen 2: Org Details\nName · ID · Contact · Address"]:::org
    end

    %% ── SITE FLOW ──
    ORG_D -->|"Sites tab"| SITE_D
    subgraph SITE_FLOW["Site Management"]
        SITE_D["Screen 4: Site Details\nGPS · Contact · Config\nTabs: Cameras | Events | Map"]:::site
    end

    %% ── CAMERA FLOW ──
    SITE_D -->|"Cameras tab"| CAM_D
    subgraph CAM_FLOW["Camera Management"]
        CAM_D["Screen 6: Camera Details\nStream Settings · Status · PTZ\nTabs: Live | Annotations | Events | Health"]:::camera
        CAM_D -->|"Open live"| LIVE["Screen 7: Live View\n+ Annotation Editor\nDraw zones · Label · Save"]:::live
        CAM_D -->|"Configure AI"| ANA["Screen 8: Analytics Config\nDetection rules · Thresholds\nROI · Schedule"]:::analytics
    end

    %% ── EVENT FLOW ──
    DASH -->|"Browse events"| EVT_L
    subgraph EVT_FLOW["Event Management"]
        EVT_L["Screen 9: Events Explorer\nCascading filters · Pagination"]:::event
        EVT_L -->|"Select event"| EVT_D["Screen 10: Event Details\nEvidence · Metadata · Resolution"]:::event
    end

    %% ── ALERT FLOW ──
    DASH -->|"Manage alerts"| ALR_L
    subgraph ALR_FLOW["Alert Management"]
        ALR_L["Screen 11: Alert Rules List\nFilter · Toggle · Test"]:::alert
        ALR_L -->|"Create / Edit"| ALR_W["Screen 12: Alert Wizard\n6-Step Form\nConditions → Channels → Escalation"]:::alert
    end

    %% ── USER FLOW ──
    DASH -->|"Manage users"| USR_L
    subgraph USR_FLOW["User Management"]
        USR_L["Screen 13: User List\nFilter by role · org"]:::user
        USR_L -->|"Add / Edit"| USR_D["Screen 14: User Details\nPersonal · Security · Permissions"]:::user
    end

    %% ── SYSTEM FLOW ──
    DASH -->|"System"| SYS_GROUP
    subgraph SYS_GROUP["System & Monitoring"]
        AUD["Screen 16: Audit Logs\nUser actions · Entity tracking"]:::system
        SYS["Screen 17: System Settings\nDeepStream · GPU · Storage · SMS"]:::system
        NTF["Screen 18: Notification History\nChannel status · Retry"]:::system
    end

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

    style DASH_GROUP fill:#E3F2FD,stroke:#1565C0,color:#000,stroke-width:2px
    style ORG_FLOW fill:#F3E5F5,stroke:#6A1B9A,color:#000,stroke-width:2px
    style SITE_FLOW fill:#E8F5E9,stroke:#2E7D32,color:#000,stroke-width:2px
    style CAM_FLOW fill:#FFF3E0,stroke:#E65100,color:#000,stroke-width:2px
    style EVT_FLOW fill:#FFFDE7,stroke:#F57F17,color:#000,stroke-width:2px
    style ALR_FLOW fill:#FFEBEE,stroke:#D32F2F,color:#000,stroke-width:2px
    style USR_FLOW fill:#E0F2F1,stroke:#00695C,color:#000,stroke-width:2px
    style SYS_GROUP fill:#ECEFF1,stroke:#455A64,color:#000,stroke-width:2px
```

---

### Screen 2 — Organization Details

| Field | Description |
|-------|-------------|
| **Org Name** | Organization display name |
| **ID** | Unique organization identifier |
| **Contact** | Email / Phone |
| **Address** | Physical address |

---

### Screen 4 — Site Details

**Sections:** Basic Info (name, org, GPS, address, timezone) → Contact (person, phone, email) → Config (settings JSON, status) → **Tabs:** Cameras | Events | Map View

---

### Screen 6 — Camera Details

```mermaid
%%{init: {'theme': 'base', 'themeVariables': { 'fontSize': '13px', 'lineColor': '#455A64' }}}%%
flowchart TD
    B["Basic Info\nName · ID · Site · RTSP"]:::orange
    B --> SS["Stream Settings\nType · Res · FPS\nBitrate · Codec"]:::blue
    SS --> ST["Status\nStream State · Pod\nHeartbeat · Active"]:::green
    ST --> AN["Analytics Config\nJSON"]:::purple
    AN --> PTZ["PTZ Config\n(conditional)"]:::red
    PTZ --> TABS["Tabs\nLive View | Annotations\nEvents | Health Metrics"]:::amber

    TABS -->|"Live View"| S7["Screen 7:\nLive + Annotations"]:::live
    TABS -->|"Configure"| S8["Screen 8:\nAnalytics Config"]:::analytics

    classDef orange fill:#E65100,stroke:#BF360C,color:#fff,stroke-width:2px
    classDef blue fill:#1565C0,stroke:#0D47A1,color:#fff,stroke-width:2px
    classDef green fill:#2E7D32,stroke:#1B5E20,color:#fff,stroke-width:2px
    classDef purple fill:#6A1B9A,stroke:#4A148C,color:#fff,stroke-width:2px
    classDef red fill:#C62828,stroke:#B71C1C,color:#fff,stroke-width:2px
    classDef amber fill:#F57F17,stroke:#E65100,color:#000,stroke-width:2px
    classDef live fill:#C62828,stroke:#B71C1C,color:#fff,stroke-width:2px
    classDef analytics fill:#AD1457,stroke:#880E4F,color:#fff,stroke-width:2px
```

---

### Screen 7 — Live View + Annotation Editor

```mermaid
%%{init: {'theme': 'base', 'themeVariables': { 'fontSize': '13px', 'lineColor': '#455A64' }}}%%
flowchart TD
    STREAM["RTSP Stream\ncameras.rtsp_url"]:::red

    STREAM -->|"Feed"| PLAYER["Video Player\nPlay/Pause · Fullscreen\nSnapshot"]:::red

    PLAYER -->|"Draw on frame"| TOOLS["Annotation Tools\nRectangle · Polygon · Line · Point\nColor · Stroke · Fill Opacity\nLabel · Zone Type\n(Detection / Tripwire / Privacy)"]:::purple

    TOOLS -->|"Save"| DB[("PostgreSQL\nannotations table")]:::infra

    DB -->|"Load existing"| LIST["Annotation List\nID · Label · Type\nZone Type · Active\nEdit / Delete"]:::blue

    LIST -->|"Edit"| TOOLS
    LIST -->|"Toggle active"| DB

    classDef red fill:#C62828,stroke:#B71C1C,color:#fff,stroke-width:2px
    classDef purple fill:#6A1B9A,stroke:#4A148C,color:#fff,stroke-width:2px
    classDef infra fill:#4527A0,stroke:#311B92,color:#fff,stroke-width:2px
    classDef blue fill:#1565C0,stroke:#0D47A1,color:#fff,stroke-width:2px
```

---

### Screen 8 — Analytics Configuration

```mermaid
%%{init: {'theme': 'base', 'themeVariables': { 'fontSize': '13px', 'lineColor': '#455A64' }}}%%
flowchart TD
    DETECT["Detection Rules\nPerson · Vehicle · LPR\nLoitering · Line Cross\nFace Recognition"]:::pink

    DETECT -->|"Set thresholds"| THRESH["Thresholds\nPerson Confidence · Vehicle Confidence\nLPR Confidence · Loitering Duration"]:::orange

    THRESH -->|"Assign zones"| ROI["ROI Assignment\nAvailable Zones (from annotations)\nZone → Rule Mapping"]:::teal

    ROI -->|"Set schedule"| SCHED["Schedule\nActive Hours Start/End\nDays of Week"]:::blue

    SCHED -->|"Save"| DB[("cameras.analytics_config\nJSON in PostgreSQL")]:::infra

    classDef pink fill:#AD1457,stroke:#880E4F,color:#fff,stroke-width:2px
    classDef orange fill:#E65100,stroke:#BF360C,color:#fff,stroke-width:2px
    classDef teal fill:#00695C,stroke:#004D40,color:#fff,stroke-width:2px
    classDef blue fill:#1565C0,stroke:#0D47A1,color:#fff,stroke-width:2px
    classDef infra fill:#4527A0,stroke:#311B92,color:#fff,stroke-width:2px
```

---

### Screen 9-10 — Events Explorer & Details

```mermaid
%%{init: {'theme': 'base', 'themeVariables': { 'fontSize': '13px', 'lineColor': '#455A64' }}}%%
flowchart LR
    subgraph S9["Screen 9 — Events Explorer"]
        F["Cascading Filters\nOrg → Site → Camera\nType · Severity · Date\nFree text · Ack status"]:::filter
        F -->|"Apply"| T["Table (11 cols)\nTime · Org · Site · Camera\nType · Severity · Confidence\nThumbnail · Video\nStatus · Ack By"]:::table
        T -->|"Row action"| A["Actions\nView · Acknowledge\nExport · Share"]:::action
        A -->|"Paginate"| P["Pagination\n10/25/50/100 per page"]:::page
    end

    A -->|"Select event"| S10

    subgraph S10["Screen 10 — Event Details"]
        EI["Event Info\nID · Timestamp · Org\nSite · Camera · Type\nSeverity · Confidence · Track ID"]:::amber
        EI --> EV["Evidence\nSnapshot Image\nVideo Clip (playback)\nThumbnail"]:::red
        EV --> MD["Metadata\nBounding Box · Object Class\nAttributes · License Plate\nFace ID"]:::purple
        MD --> AS["Alert Status\nSent · Methods · Ack By/At\nResolved At · Note"]:::green
        AS --> ACT["Actions\nAcknowledge · Resolve\nAdd Note · Download"]:::blue
    end

    classDef filter fill:#F57F17,stroke:#E65100,color:#fff,stroke-width:2px
    classDef table fill:#FFF9C4,stroke:#FBC02D,color:#000,stroke-width:1px
    classDef action fill:#F57F17,stroke:#E65100,color:#fff,stroke-width:2px
    classDef page fill:#37474F,stroke:#263238,color:#fff,stroke-width:2px
    classDef amber fill:#F57F17,stroke:#E65100,color:#000,stroke-width:2px
    classDef red fill:#C62828,stroke:#B71C1C,color:#fff,stroke-width:2px
    classDef purple fill:#6A1B9A,stroke:#4A148C,color:#fff,stroke-width:2px
    classDef green fill:#2E7D32,stroke:#1B5E20,color:#fff,stroke-width:2px
    classDef blue fill:#1565C0,stroke:#0D47A1,color:#fff,stroke-width:2px

    style S9 fill:#FFFDE7,stroke:#F57F17,color:#000,stroke-width:2px
    style S10 fill:#FFF8E1,stroke:#FFB300,color:#000,stroke-width:2px
```

---

### Screen 12 — Alert Rule Wizard (6-Step Flow)

```mermaid
%%{init: {'theme': 'base', 'themeVariables': { 'fontSize': '13px', 'lineColor': '#455A64' }}}%%
flowchart LR
    S1["Step 1\nBasic Info\nName · Desc\nOrg · Site · Camera"]:::step1

    S1 -->|"Next"| S2["Step 2\nEvent Conditions\nTypes (multi)\nSeverity threshold\nConfidence (slider)\nCooldown (sec)"]:::step2

    S2 -->|"Next"| S3["Step 3\nTime Schedule\nStart/End time\nDays of week"]:::step3

    S3 -->|"Next"| S4["Step 4\nChannels\nSMS · WhatsApp\nVoice · Email\nWebhook"]:::step4

    S4 -->|"Next"| S5["Step 5\nChannel Config\nWhatsApp template\nSMS template\nCall message\nWebhook URL/headers"]:::step5

    S5 -->|"Next"| S6["Step 6\nEscalation\nEnable toggle\nDelay (sec)\nEscalation rule"]:::step6

    S6 -->|"Save"| DB[("alert_rules\nPostgreSQL")]:::infra

    classDef step1 fill:#D32F2F,stroke:#B71C1C,color:#fff,stroke-width:2px
    classDef step2 fill:#E65100,stroke:#BF360C,color:#fff,stroke-width:2px
    classDef step3 fill:#F57F17,stroke:#E65100,color:#000,stroke-width:2px
    classDef step4 fill:#2E7D32,stroke:#1B5E20,color:#fff,stroke-width:2px
    classDef step5 fill:#1565C0,stroke:#0D47A1,color:#fff,stroke-width:2px
    classDef step6 fill:#6A1B9A,stroke:#4A148C,color:#fff,stroke-width:2px
    classDef infra fill:#4527A0,stroke:#311B92,color:#fff,stroke-width:2px
```

---

### Screen 13-14 — User Management

| Column (Screen 13) | Source |
|--------|--------|
| Name | `users.first_name` + `users.last_name` |
| Email | `users.email` |
| Organization | `organizations.name` |
| Role | `users.role` |
| Phone | `users.phone` |
| MFA Enabled | `users.mfa_enabled` |
| Last Login | `users.last_login` |
| Status | `users.is_active` |

**Screen 13 Actions:** Edit · Delete · Reset Password · Impersonate (admin)

**Screen 14 Sections:** Personal Info (name, email, phone, org) → Security (role, permissions JSON, MFA toggle, password) → Status (active toggle)

---

### Screen 15 — Dashboard (Widget Flow)

```mermaid
%%{init: {'theme': 'base', 'themeVariables': { 'fontSize': '13px', 'lineColor': '#455A64' }}}%%
flowchart TD
    subgraph METRICS["Metrics Cards Row"]
        direction LR
        M1["Total\nCameras"]:::blue
        M2["Active\nStreams"]:::green
        M3["Events\nToday"]:::amber
        M4["Critical\nAlerts"]:::red
        M5["GPU\nUtilization"]:::purple
        M6["Storage\nUsed"]:::teal
    end

    METRICS -->|"Summary data"| ORG_SUM["Org Summary Table\nOrg Name · Camera Count\nEvents 24hr · Critical Alerts"]:::blueLight

    ORG_SUM -->|"Real-time"| FEED["Live Events Feed\nTime · Camera · Type\nSeverity · Thumbnail"]:::redLight

    FEED -->|"Hourly data"| CHART["Event Trend Chart\nPerson · Vehicle · Line Cross\nSource: event_summary_hourly"]:::amberLight

    CHART -->|"Ranked"| TOP["Top Cameras (24hr)\nCamera Name · Site\nEvent Count"]:::orangeLight

    TOP -->|"Infra data"| HEALTH["System Health\nPod Name · Status\nStreams/Max · Heartbeat"]:::greenLight

    HEALTH -->|"Geo data"| MAP["Map View\nSite pins · Alert indicators\nRed (critical) · Yellow (warning)"]:::tealLight

    classDef blue fill:#1565C0,stroke:#0D47A1,color:#fff,stroke-width:2px
    classDef green fill:#2E7D32,stroke:#1B5E20,color:#fff,stroke-width:2px
    classDef amber fill:#F57F17,stroke:#E65100,color:#000,stroke-width:2px
    classDef red fill:#C62828,stroke:#B71C1C,color:#fff,stroke-width:2px
    classDef purple fill:#6A1B9A,stroke:#4A148C,color:#fff,stroke-width:2px
    classDef teal fill:#00695C,stroke:#004D40,color:#fff,stroke-width:2px
    classDef blueLight fill:#BBDEFB,stroke:#1565C0,color:#000,stroke-width:2px
    classDef redLight fill:#FFCDD2,stroke:#E53935,color:#000,stroke-width:2px
    classDef amberLight fill:#FFF9C4,stroke:#FBC02D,color:#000,stroke-width:2px
    classDef orangeLight fill:#FFE0B2,stroke:#FF9800,color:#000,stroke-width:2px
    classDef greenLight fill:#C8E6C9,stroke:#43A047,color:#000,stroke-width:2px
    classDef tealLight fill:#B2DFDB,stroke:#00897B,color:#000,stroke-width:2px

    style METRICS fill:#E3F2FD,stroke:#1565C0,color:#000,stroke-width:2px
```

---

### Screens 16-18 — System Screens

**Screen 16 — Audit Logs:** Timestamp · User · Org · Action · Entity Type/ID · IP · Old/New Values (expandable)

**Filters:** User, Org, Action (multi), Entity Type (multi), Date Range | **Export:** CSV

---

**Screen 17 — System Settings (Connected Sections):**

```mermaid
%%{init: {'theme': 'base', 'themeVariables': { 'fontSize': '13px', 'lineColor': '#455A64' }}}%%
flowchart LR
    GEN["General\nPlatform Name\nLogo URL\nDefault Timezone"]:::purple

    GEN -->|"Configure"| DS["DeepStream\nBatch Size\nWatchdog Interval\nRetention Days\nClip Duration"]:::red

    DS -->|"Set capacity"| GPU["GPU Capacities\nL40S Max Streams\nT4 Max Streams\nA100 Max Streams"]:::green

    GPU -->|"Storage config"| STR["Storage\nType: S3/MinIO/Local\nBucket · Endpoint\nKeys (encrypted)"]:::blue

    STR -->|"Comms config"| SMS["SMS Provider\nTwilio/Vonage\nSID · Token\nFrom Number"]:::orange

    SMS -->|"WhatsApp"| WA["WhatsApp\nInterakt/Gupshup\nAPI Key\nBusiness Phone"]:::teal

    WA -->|"Save all"| DB[("system_config\nkey-value pairs")]:::infra

    classDef purple fill:#4527A0,stroke:#311B92,color:#fff,stroke-width:2px
    classDef red fill:#C62828,stroke:#B71C1C,color:#fff,stroke-width:2px
    classDef green fill:#2E7D32,stroke:#1B5E20,color:#fff,stroke-width:2px
    classDef blue fill:#1565C0,stroke:#0D47A1,color:#fff,stroke-width:2px
    classDef orange fill:#E65100,stroke:#BF360C,color:#fff,stroke-width:2px
    classDef teal fill:#00695C,stroke:#004D40,color:#fff,stroke-width:2px
    classDef infra fill:#4527A0,stroke:#311B92,color:#fff,stroke-width:2px
```

**Screen 18 — Notification History:**

| Column | Source |
|--------|--------|
| Created At | `notification_queue.created_at` |
| Event ID | `notification_queue.event_id` |
| Channel | `notification_queue.channel` |
| Recipient | `notification_queue.recipient` |
| Content | `notification_queue.content` |
| Status | `notification_queue.status` (pending/sent/failed) |
| Retry Count | `notification_queue.retry_count` |
| Sent At | `notification_queue.sent_at` |
| Error Message | `notification_queue.error_message` |

**Actions:** Retry (failed) · View Details

---

## 2. Backend — Layer 0: Ingestion

```mermaid
%%{init: {'theme': 'base', 'themeVariables': { 'fontSize': '14px', 'lineColor': '#455A64' }}}%%
flowchart LR
    CAM1["IP Camera 1\nRTSP/H.264"]:::cam
    CAM2["IP Camera 2\nRTSP/H.264"]:::cam
    CAMN["IP Camera N\nRTSP/H.264"]:::cam

    ONVIF["ONVIF\nDiscovery\nService"]:::discover

    CAM1 -->|"ONVIF Probe"| ONVIF
    CAM2 -->|"ONVIF Probe"| ONVIF
    CAMN -->|"ONVIF Probe"| ONVIF

    ONVIF -->|"Camera\ndetected"| VST["VST\nCamera\nManagement"]:::vst

    VST -->|"INSERT camera\ntaken=FALSE\nstream_state=ON"| PG[("PostgreSQL")]:::db

    VST -->|"Publish\ncamera_streaming"| REDIS[("Redis\nPub/Sub")]:::redis

    CAM1 -.->|"RTSP Stream"| PODS["DeepStream\nPods"]:::pod
    CAM2 -.->|"RTSP Stream"| PODS
    CAMN -.->|"RTSP Stream"| PODS

    classDef cam fill:#0D47A1,stroke:#0D47A1,color:#fff,stroke-width:2px
    classDef discover fill:#00897B,stroke:#004D40,color:#fff,stroke-width:2px
    classDef vst fill:#2E7D32,stroke:#1B5E20,color:#fff,stroke-width:2px
    classDef db fill:#4527A0,stroke:#311B92,color:#fff,stroke-width:2px
    classDef redis fill:#C62828,stroke:#B71C1C,color:#fff,stroke-width:2px
    classDef pod fill:#B71C1C,stroke:#B71C1C,color:#fff,stroke-width:2px
```

---

## 3. Backend — Layer 1: Control Plane

```mermaid
%%{init: {'theme': 'base', 'themeVariables': { 'fontSize': '14px', 'lineColor': '#455A64' }}}%%
flowchart TD
    %% ── COMPONENTS ──
    VST["VST\nCamera Management\nRegister cameras\nPublish events"]:::vst
    REDIS[("Redis\nPub/Sub\nMessage Bus")]:::redis
    SDR["SDR\nSensor Distribution Router\nCalculate load\nAssign cameras"]:::sdr
    PG[("PostgreSQL\nCamera Registry\nSource of Truth")]:::db
    WDM["WDM\nAuto-Scaler\nMonitor capacity\nScale replicas"]:::wdm
    K8S["Kubernetes\nAPI Server"]:::k8s
    GPU_OP["GPU Operator\nProvision L40S/T4/A100"]:::k8s
    WDG["Watchdog\n(per pod)\nReconcile every 10s"]:::wdg
    POD["DeepStream Pod\nGPU Pipeline\nREST API :9010"]:::pod

    %% ═══ FLOW ═══
    VST -->|"INSERT camera\ntaken=FALSE"| PG
    VST -->|"Publish\ncamera_streaming"| REDIS

    REDIS -->|"Event\nnotification"| SDR

    SDR -->|"Query available\npods + capacity"| PG
    SDR -->|"Assign camera\nto pod"| PG
    SDR -->|"Capacity\nexceeded?"| WDM

    WDM -->|"Scale replicas\nWDM_MAX_REPLICAS"| K8S
    K8S -->|"Provision\nnew GPU node"| GPU_OP
    GPU_OP -->|"Node + GPU\nready"| POD

    WDG -->|"SELECT WHERE\ntaken=FALSE\nLIMIT capacity"| PG
    PG -->|"Return\nunclaimed cameras"| WDG
    WDG -->|"POST /stream/add\nDELETE /stream/remove"| POD
    WDG -->|"UPDATE\ntaken=TRUE\nassigned_pod"| PG
    WDG -->|"Heartbeat\ncurrent_streams\nlast_heartbeat"| PG

    classDef vst fill:#2E7D32,stroke:#1B5E20,color:#fff,stroke-width:2px
    classDef redis fill:#C62828,stroke:#B71C1C,color:#fff,stroke-width:2px
    classDef sdr fill:#00695C,stroke:#004D40,color:#fff,stroke-width:2px
    classDef db fill:#4527A0,stroke:#311B92,color:#fff,stroke-width:2px
    classDef wdm fill:#6A1B9A,stroke:#4A148C,color:#fff,stroke-width:2px
    classDef k8s fill:#1565C0,stroke:#0D47A1,color:#fff,stroke-width:2px
    classDef wdg fill:#F57F17,stroke:#E65100,color:#000,stroke-width:2px
    classDef pod fill:#B71C1C,stroke:#B71C1C,color:#fff,stroke-width:2px
```

### Component Specifications

| Component | Trigger | Action | Output |
|-----------|---------|--------|--------|
| **VST** | ONVIF probe | Write DB + publish Redis | Camera registered (`taken=FALSE`) |
| **SDR** | Redis `camera_streaming` | Calculate pods, assign cameras | Assignment decision |
| **WDM** | Capacity exceeded | K8s scale-up | New GPU pod provisioned |
| **Watchdog** | Timer (10s) | Remove stale / add new / heartbeat | Pipeline updated via REST |

---

## 4. Backend — Layer 2: Data Plane (DeepStream Pipeline)

```mermaid
%%{init: {'theme': 'base', 'themeVariables': { 'fontSize': '14px', 'lineColor': '#455A64' }}}%%
flowchart TD
    %% ── WATCHDOG CONTROL ──
    WDG["Watchdog\nReconciler"]:::wdg

    %% ── PIPELINE ──
    SRC["nvmultiurisrcbin\nUp to 50 RTSP Sources"]:::src
    MUX["nvstreammux\nbatch-size = 40"]:::mux
    INF["nvinfer\nYOLO Detection\n(GPU Inference)"]:::infer
    TEE["tee — COMMON BUFFER\nSingle inference → 3 outputs"]:::tee

    %% ── RECORDING ──
    Q1["Queue"]:::recQ --> ENC1["H264\nEncoder"]:::rec --> MUX1["MP4\nMux"]:::rec --> SINK1["File\nSink"]:::rec --> S3["S3 / MinIO\nObject Storage"]:::out_s3

    %% ── STREAMING ──
    Q2["Queue"]:::strQ --> ENC2["H264\nEncoder"]:::str --> MUX2["FLV\nMux"]:::str --> SINK2["RTMP\nSink"]:::str --> RTMP["RTMP Server\nHLS / DASH"]:::out_rtmp

    %% ── ANALYTICS ──
    Q3["Queue"]:::anaQ --> CONV["nvmsgconv\nMeta → JSON"]:::ana --> BROKER["nvmsg\nbroker"]:::ana --> SINK3["Kafka\nSink"]:::ana --> KAFKA["Apache Kafka\nEvent Topics"]:::out_kafka

    %% ═══ CONNECTIONS ═══
    WDG -->|"POST /stream/add\nDELETE /stream/remove\nREST :9010"| SRC

    SRC -->|"Decoded\nframes"| MUX
    MUX -->|"Batched\nframes"| INF
    INF -->|"Inferred frames\n+ metadata"| TEE

    TEE -->|"Recording\nbranch"| Q1
    TEE -->|"Streaming\nbranch"| Q2
    TEE -->|"Analytics\nbranch"| Q3

    classDef wdg fill:#F57F17,stroke:#E65100,color:#000,stroke-width:3px
    classDef src fill:#0D47A1,stroke:#0D47A1,color:#fff,stroke-width:2px
    classDef mux fill:#1565C0,stroke:#0D47A1,color:#fff,stroke-width:2px
    classDef infer fill:#B71C1C,stroke:#B71C1C,color:#fff,stroke-width:3px
    classDef tee fill:#E65100,stroke:#BF360C,color:#fff,stroke-width:3px
    classDef recQ fill:#2E7D32,stroke:#1B5E20,color:#fff,stroke-width:1px
    classDef rec fill:#2E7D32,stroke:#1B5E20,color:#fff,stroke-width:2px
    classDef strQ fill:#6A1B9A,stroke:#4A148C,color:#fff,stroke-width:1px
    classDef str fill:#6A1B9A,stroke:#4A148C,color:#fff,stroke-width:2px
    classDef anaQ fill:#00695C,stroke:#004D40,color:#fff,stroke-width:1px
    classDef ana fill:#00695C,stroke:#004D40,color:#fff,stroke-width:2px
    classDef out_s3 fill:#1B5E20,stroke:#0D3B0E,color:#fff,stroke-width:2px
    classDef out_rtmp fill:#4A148C,stroke:#311B92,color:#fff,stroke-width:2px
    classDef out_kafka fill:#004D40,stroke:#002622,color:#fff,stroke-width:2px
```

### Pipeline Element Reference

| Element | Plugin | Purpose | Config |
|---------|--------|---------|--------|
| **Source** | `nvmultiurisrcbin` | Ingest RTSP streams | Up to 50 sources/pod |
| **Muxer** | `nvstreammux` | Batch frames for GPU | `batch-size=40` |
| **Inference** | `nvinfer` | YOLO detection | Object detection + classification |
| **Splitter** | `tee` | Common buffer split | 3 branches |
| **Recording** | `h264enc` → `mp4mux` → `filesink` | Store video | → S3/MinIO |
| **Streaming** | `h264enc` → `flvmux` → `rtmpsink` | Live delivery | → RTMP Server |
| **Analytics** | `nvmsgconv` → `nvmsgbroker` → Kafka | Event publish | → Kafka topic |

---

## 5. Backend — Layer 3: Output

```mermaid
%%{init: {'theme': 'base', 'themeVariables': { 'fontSize': '14px', 'lineColor': '#455A64' }}}%%
flowchart LR
    %% ── SOURCES ──
    POD1["Pod 1\nDeepStream"]:::pod
    POD2["Pod 2\nDeepStream"]:::pod
    PODN["Pod N\nDeepStream"]:::pod

    %% ── OUTPUT LAYER ──
    KAFKA["Apache Kafka\nAnalytics Events\nJSON"]:::kafka
    S3["S3 / MinIO\nRecordings\n.mp4"]:::s3
    RTMP["RTMP Server\nHLS / DASH\nLive Streams"]:::rtmp
    PROM["Prometheus\nGPU/CPU\nMetrics"]:::prom

    %% ── CONSUMERS ──
    DASH["Dashboard\nReact/Vue"]:::consumer
    ALERT["Alert Manager\nSMS · WhatsApp\nEmail · Webhook"]:::consumer
    PLAYER["Video Player\nWebRTC"]:::consumer
    GRAFANA["Grafana\nDashboards"]:::consumer

    %% ═══ CONNECTIONS ═══
    POD1 -->|"JSON events"| KAFKA
    POD2 -->|"JSON events"| KAFKA
    PODN -->|"JSON events"| KAFKA

    POD1 -->|".mp4 files"| S3
    POD2 -->|".mp4 files"| S3
    PODN -->|".mp4 files"| S3

    POD1 -->|"FLV stream"| RTMP
    POD2 -->|"FLV stream"| RTMP
    PODN -->|"FLV stream"| RTMP

    POD1 -.->|"Metrics"| PROM
    POD2 -.->|"Metrics"| PROM
    PODN -.->|"Metrics"| PROM

    KAFKA -->|"Events feed"| DASH
    KAFKA -->|"Alert trigger"| ALERT
    S3 -->|"Video clips"| PLAYER
    RTMP -->|"Live stream"| PLAYER
    PROM -->|"Metrics"| GRAFANA

    classDef pod fill:#B71C1C,stroke:#B71C1C,color:#fff,stroke-width:2px
    classDef kafka fill:#00695C,stroke:#004D40,color:#fff,stroke-width:2px
    classDef s3 fill:#2E7D32,stroke:#1B5E20,color:#fff,stroke-width:2px
    classDef rtmp fill:#6A1B9A,stroke:#4A148C,color:#fff,stroke-width:2px
    classDef prom fill:#E65100,stroke:#BF360C,color:#fff,stroke-width:2px
    classDef consumer fill:#1565C0,stroke:#0D47A1,color:#fff,stroke-width:2px
```

---

## 6. Auto-Scaling Flow

> Scaling from 100 → 200 cameras automatically

```mermaid
%%{init: {'theme': 'base', 'themeVariables': { 'fontSize': '14px', 'lineColor': '#455A64' }}}%%
flowchart TD
    T0["T0: STEADY STATE\n100 cameras · 2 GPUs\nPod 1: 50 cams (100%)\nPod 2: 50 cams (100%)"]:::steady

    T0 -->|"100 new cameras\ndetected via ONVIF"| T1["T1: CAMERAS DETECTED\nVST writes to DB (taken=FALSE)\nSDR receives Redis event\nSDR calculates: need 4 pods\nCurrently 2 → need +2"]:::detect

    T1 -->|"Capacity exceeded"| T2["T2: SCALE TRIGGERED\nWDM: current=100, desired=200\nWDM_MAX_REPLICAS = 3 (min)\nK8s scales to 4 replicas\nGPU Operator provisions nodes"]:::scale

    T2 -->|"GPU nodes provisioned"| T3["T3: NEW PODS START\nPod 3 (GPU 2, 0 cams)\nPod 4 (GPU 3, 0 cams)\nWatchdog services begin"]:::newpods

    T3 -->|"Watchdog runs\nevery 10s"| T4["T4: RECONCILIATION\nPod 3: SELECT taken=FALSE LIMIT 50\nPod 4: SELECT taken=FALSE LIMIT 50\nPOST /stream/add for each\nUPDATE taken=TRUE"]:::reconcile

    T4 -->|"All cameras\nassigned"| T5["T5: BALANCED STATE\nPod 1: 50 cams (100%)\nPod 2: 50 cams (100%)\nPod 3: 50 cams (100%) ← new\nPod 4: 50 cams (100%) ← new\nTotal: 200/200"]:::balanced

    T5 -->|"Continuous"| T6["T6: MAINTENANCE\nWatchdog continues per pod\nPhase 1: Remove stale\nPhase 2: Claim new\nPhase 3: Heartbeat"]:::maintain

    classDef steady fill:#1565C0,stroke:#0D47A1,color:#fff,stroke-width:2px
    classDef detect fill:#F57F17,stroke:#E65100,color:#000,stroke-width:2px
    classDef scale fill:#C62828,stroke:#B71C1C,color:#fff,stroke-width:2px
    classDef newpods fill:#6A1B9A,stroke:#4A148C,color:#fff,stroke-width:2px
    classDef reconcile fill:#E65100,stroke:#BF360C,color:#fff,stroke-width:2px
    classDef balanced fill:#2E7D32,stroke:#1B5E20,color:#fff,stroke-width:3px
    classDef maintain fill:#00695C,stroke:#004D40,color:#fff,stroke-width:2px
```

---

## 7. Watchdog Reconciliation Loop

```mermaid
%%{init: {'theme': 'base', 'themeVariables': { 'fontSize': '14px', 'lineColor': '#455A64' }}}%%
flowchart TD
    START["TIMER TICK\n(every 10 seconds)"]:::timer

    START -->|"Phase 1"| P1_Q["SELECT source_id\nWHERE assigned_pod = THIS\nAND stream_state IN\n('OFF', 'BAD_STREAM')"]:::queryRed

    P1_Q -->|"For each stale"| P1_A["DELETE /stream/remove/{id}\nUPDATE SET taken=FALSE\nassigned_pod = NULL"]:::actionRed

    P1_A -->|"Phase 2"| P2_C["Calculate\navailable_capacity =\nmax_capacity - current_count"]:::calcBlue

    P2_C -->|"Query DB"| P2_Q["SELECT source_id, rtsp_url\nWHERE stream_state='ON'\nAND taken=FALSE\nLIMIT available_capacity"]:::queryBlue

    P2_Q -->|"For each new"| P2_A["POST /stream/add\nUPDATE SET taken=TRUE\nassigned_pod = THIS_POD"]:::actionBlue

    P2_A -->|"Phase 3"| P3["UPDATE pods SET\ncurrent_streams = count\nlast_heartbeat = NOW()\nWHERE pod_name = THIS"]:::heartbeat

    P3 -->|"Sleep 10s"| START

    classDef timer fill:#37474F,stroke:#263238,color:#fff,stroke-width:3px
    classDef queryRed fill:#C62828,stroke:#B71C1C,color:#fff,stroke-width:2px
    classDef actionRed fill:#EF5350,stroke:#E53935,color:#fff,stroke-width:2px
    classDef calcBlue fill:#0277BD,stroke:#01579B,color:#fff,stroke-width:2px
    classDef queryBlue fill:#1565C0,stroke:#0D47A1,color:#fff,stroke-width:2px
    classDef actionBlue fill:#1976D2,stroke:#1565C0,color:#fff,stroke-width:2px
    classDef heartbeat fill:#F57F17,stroke:#E65100,color:#000,stroke-width:2px
```

---

## 8. End-to-End Data Flow

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
    participant S3 as S3
    participant RTMP as RTMP

    rect rgb(200, 230, 201)
        Note over CAM,RTMP: PHASE 1 — CAMERA REGISTRATION
        CAM->>VST: ONVIF Probe
        VST->>DB: INSERT camera (taken=FALSE)
        VST->>RD: Publish "camera_streaming"
        RD->>SDR: Event notification
        SDR->>DB: Query pods, assign camera
    end

    rect rgb(255, 224, 178)
        Note over CAM,RTMP: PHASE 2 — AUTO-SCALING
        SDR->>K8: Scale request
        K8->>DS: Provision GPU Pod
    end

    rect rgb(187, 222, 251)
        Note over CAM,RTMP: PHASE 3 — WATCHDOG RECONCILIATION
        loop Every 10 seconds
            WD->>DB: SELECT unclaimed cameras
            DB-->>WD: Return cameras
            WD->>DS: POST /api/v1/stream/add
            DS-->>WD: OK
            WD->>DB: UPDATE taken=TRUE
        end
    end

    rect rgb(252, 228, 236)
        Note over CAM,RTMP: PHASE 4 — CONTINUOUS STREAMING
        CAM->>DS: RTSP Stream (H.264, 30fps)
        DS->>DS: Decode → Infer → Common Buffer → Split
        par Recording
            DS->>S3: H264Enc → MP4Mux → .mp4
        and Streaming
            DS->>RTMP: H264Enc → FLVMux → live
        and Analytics
            DS->>KF: nvmsgconv → Kafka (JSON)
        end
    end
```

---

## Component Interaction Summary

| Component | Trigger | Action | Output |
|-----------|---------|--------|--------|
| **VST** | ONVIF probe | Write DB + Redis event | Camera registered |
| **SDR** | Redis event | Calculate + assign | Assignment decision |
| **K8s/WDM** | Capacity exceeded | Provision GPU node | New pod |
| **Watchdog** | Timer (10s) | Add/remove via REST | Pipeline updated |
| **DeepStream** | REST API call | Modify running pipeline | Stream added/removed |
| **Pipeline** | Frame arrival | Detect → Split → Output | Recording + Streaming + Analytics |

## Key Design Principles

| Principle | How It Works |
|-----------|-------------|
| **Common Buffer** | Single GPU inference feeds 3 outputs via `tee` — no redundant processing |
| **Decentralized Reconciliation** | Each pod runs its own watchdog independently |
| **Database as Source of Truth** | `taken` flag prevents double-assignment across pods |
| **K8s GPU Orchestration** | GPU Operator + WDM_MAX_REPLICAS scales hardware |
| **Event + Polling Hybrid** | Redis for immediate events, watchdog for reconciliation |
| **Location Awareness** | Cameras assigned to geographically closest pods |
| **Self-Healing** | Components fail independently; system auto-recovers |
