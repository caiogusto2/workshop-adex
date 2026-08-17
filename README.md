# 🚀 Oracle ADEX Workshops

Welcome to the **Oracle ADEX Workshops** repository!

This repository contains hands-on labs, examples, presentations, datasets, and supporting materials designed to explore Oracle Cloud Infrastructure (OCI) data and AI technologies through practical exercises.

The workshops focus on building real-world data architectures using services such as **OCI AI Data Platform**, **Oracle Autonomous Database**, **OCI Data Flow**, **Apache Spark**, and other OCI data services.

---

## 🎯 Objectives

The goal of this repository is to provide practical, hands-on experiences for working with Oracle Cloud data technologies.

Throughout the workshops, you will explore how to:

- Prepare and configure OCI environments for data workloads.
- Build end-to-end data pipelines.
- Process and transform data using Apache Spark and PySpark.
- Work with Oracle Autonomous Database.
- Build Bronze, Silver, and Gold data layers using the Medallion Architecture.
- Perform data quality checks and exploratory data analysis.
- Develop Spark applications using OCI AI Data Platform.
- Integrate VS Code with OCI development environments.
- Adapt Spark applications for execution on OCI Data Flow.
- Explore AI and data capabilities available across Oracle Cloud Infrastructure.

---

## 🧪 Workshops

The repository is organized into different hands-on labs and supporting materials.

### 🤖 OCI AI Data Platform

Hands-on workshop focused on building an end-to-end data pipeline using **OCI AI Data Platform (AIDP)**, **Apache Spark**, **Oracle Autonomous Database**, and **OCI Data Flow**.

During this lab, you will:

- Prepare the OCI AI Data Platform infrastructure.
- Create a workspace and Spark cluster.
- Integrate AIDP with VS Code.
- Load and explore datasets.
- Perform data quality analysis.
- Build Bronze, Silver, and Gold layers.
- Store processed data in Autonomous Database.
- Explore Spark execution plans, logs, and metrics.
- Adapt applications developed in AIDP for deployment on OCI Data Flow.

👉 [Start the OCI AI Data Platform workshop](./aidataplatform/aidataplatform.md)

---

### 🗄️ Autonomous Database

Hands-on materials related to **Oracle Autonomous Database**, including examples and exercises for working with data directly on OCI.

👉 [Explore the Autonomous Database materials](./autonomousdb/)

---

### 🧰 Environment Configuration

Supporting material for preparing the environment before starting the hands-on labs.

👉 [Environment configuration](./0-pt-config-lab/)

---

## 🏗️ Architecture

Some workshops in this repository demonstrate an end-to-end data architecture similar to:

```text
                    Oracle Cloud Infrastructure

       ┌─────────────────────────────────────────────┐
       │              Object Storage                 │
       │             Raw / Source Data               │
       └─────────────────────┬───────────────────────┘
                             │
                             ▼
       ┌─────────────────────────────────────────────┐
       │          OCI AI Data Platform               │
       │                                             │
       │     VS Code + Workspace + Spark Cluster     │
       │                                             │
       │   Bronze  →  Silver  →  Gold               │
       └─────────────────────┬───────────────────────┘
                             │
                             ▼
       ┌─────────────────────────────────────────────┐
       │        Oracle Autonomous Database           │
       │          Curated / Analytics Data           │
       └─────────────────────┬───────────────────────┘
                             │
                             ▼
       ┌─────────────────────────────────────────────┐
       │              OCI Data Flow                  │
       │                                             │
       │     Production Spark Application            │
       └─────────────────────────────────────────────┘
```

The objective is to demonstrate the complete development lifecycle: from **data ingestion and experimentation** to **data transformation, persistence, analysis, and Spark application deployment**.

---

## 🥉 Bronze → 🥈 Silver → 🥇 Gold

Several exercises use the **Medallion Architecture** to organize the data pipeline.

| Layer | Purpose |
|---|---|
| 🥉 **Bronze** | Raw data ingestion and initial persistence |
| 🥈 **Silver** | Data cleaning, transformation, enrichment, and integration |
| 🥇 **Gold** | Aggregated and business-ready datasets |

This approach allows participants to understand how data can progressively evolve from its original format into datasets ready for analytics and AI workloads.

---

## 🛠️ Technologies

The workshops may use the following technologies and OCI services:

- **Oracle Cloud Infrastructure (OCI)**
- **OCI AI Data Platform**
- **Oracle Autonomous Database**
- **OCI Data Flow**
- **OCI Object Storage**
- **Apache Spark**
- **PySpark**
- **Python**
- **SQL**
- **Visual Studio Code**
- **Git / GitHub**

---

## 📂 Repository Structure

```text
workshop-adex/
│
├── 0-pt-config-lab/       # Environment preparation
├── aidataplatform/        # OCI AI Data Platform workshop
│   ├── arquivos_csv/      # Workshop datasets
│   ├── images/            # Workshop screenshots
│   └── aidataplatform.md  # AIDP hands-on guide
│
├── autonomousdb/          # Autonomous Database materials
├── slides/                # Workshop presentations
├── workshops/             # Additional hands-on labs
├── index.html             # Workshop web entry point
└── README.md              # Repository overview
```
---

## ⚠️ Important

These workshops create resources in **Oracle Cloud Infrastructure**.

Before starting:

- Make sure you have access to an OCI tenancy.
- Verify that you have the necessary permissions to create the resources required by the selected workshop.
- Follow the naming conventions described in each hands-on lab.
- Remove resources after completing the workshop when they are no longer required.

> Resource availability, interfaces, features, and service behavior may change over time. Refer to the official Oracle Cloud Infrastructure documentation when necessary.

---

## 👥 Authors

**Author**

- Caio Oliveira

**Contributors**

- Isabelle Anjos

---

## 🛡️ Safe Harbor

The workshops and materials in this repository are intended for informational and educational purposes.

They do not represent a commitment to deliver any material, code, or functionality and should not be relied upon when making purchasing decisions.

The development, release, timing, availability, and pricing of any features or functionality described for Oracle products remain at the sole discretion of Oracle Corporation.

---

### _Enjoy your Oracle Cloud experience!_ ☁️