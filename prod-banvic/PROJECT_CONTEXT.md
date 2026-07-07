# Project Context: BanVic Data Engineer Certification

## Introduction
This certification challenge evaluates the ability to plan and develop data infrastructure and ingestion flows. The goal is to build a Proof of Concept (PoC) for data infrastructure and pipelines, centralizing information for easy access using techniques and tools taught in the course.

## Context: Banco Vitória S.A. (BanVic)
*   **Founding:** Established in São Paulo in 2010, aiming for efficient banking services both physically and digitally.
*   **Team:** 100 dedicated employees; has grown into a prominent national financial institution.
*   **Vision:** Focused on transparent and convenient banking experiences.
*   **Current Need:** CEO Sofia Oliveira recognizes the need for data-driven decisions to enhance products and services.
*   **Key Stakeholders & Perspectives:**
    *   **Sofia Oliveira (CEO):** Believes data is key to elevating the bank; wants a dashboard for the commercial area and to identify investment levers.
    *   **André Tech (CTO):** Enthusiastic about advanced data analytics to optimize internal operations, wants to move beyond manual analyses.
    *   **Camila Diniz (Commercial Director):** Skeptical, prefers traditional marketing investments and client segmentation. Her team holds critical commercial data, posing potential bureaucratic risks to the project.
    *   **Lucas Johnson (Data Analyst):** Proposes a pilot project focusing on credit data to demonstrate value and convince Camila.
*   **Data Landscape:** ERP, CRM, and marketing data reside on a cloud server. Current analyses are manual (spreadsheets/presentations). Open to BI tools like PowerBI or Databricks AI/BI.
*   **Commercial Area Goal:** Increase transactions per client, maintain active clients, and reduce churn.
*   **CEO's Investment Goal:** Evaluate historical data to find quantitative, impactful levers for investment in the next quarter.

## Challenge Objective
The primary goal is to build a data ingestion pipeline and infrastructure, enabling analysts to consume data centrally. The initial data source will be 7 tables from the ERP, provided in `banvic_data.zip`.

## Challenge Steps (Key Deliverables)

1.  **Infrastructure as Code (IaC), Containerization, and Kubernetes:**
    *   Prepare execution (Airflow) and storage (PostgreSQL or MinIO) structures.
    *   Utilize a local Kubernetes environment (MiniKube/Kind).

2.  **ELT Pipeline Development:**
    *   Use `banvic_data.zip` (simulating legacy/on-premise systems) as the data source.
    *   Build an ingestion pipeline using **Meltano** or **Embulk** for extraction and loading.
    *   Load data into a simulated Data Lake/Warehouse (Postgres or MinIO).
    *   Configure Extractors (Taps) and Loaders (Targets) for data integrity.

3.  **Task Orchestration:**
    *   Develop DAGs in **Apache Airflow** to orchestrate ingestion pipelines.
    *   DAGs must have well-defined tasks, respecting dependencies.
    *   Utilize Sensors for file availability checks if necessary.

4.  **Monitoring and Fault Handling:**
    *   Implement basic monitoring strategies.
    *   Orchestration should handle failures (retries) and ensure idempotence.

5.  **Technical Documentation:**
    *   Document the solution's architecture.
    *   Explain tool choices.
    *   Provide instructions for local project execution.

6.  **Final Presentation and Video:**
    *   Create a presentation and record a 3-5 minute video explaining the solution.

## Evaluation Criteria
*   **Infrastructure Domain:** Ability to set up the environment using containers (Docker), IaC, and Kubernetes (isolation, reproducibility).
*   **Data Ingestion Implementation:** Correct configuration of ingestion tools, understanding source/destination connections, and efficient data movement.
*   **Orchestration and Airflow Best Practices:** Organized DAGs, correct Operator usage, defined dependencies, proper scheduling.
*   **Code Quality and Resilience:** Clean, modular, error-handling code.
*   **Security and Secret Management:** No exposure of credentials in code.
*   **Solution Presentation:** Clarity in architectural explanation and functional demonstration.

## Final Deliverables
*   **Git Repository (Link or Zip):** Containing all project code (infrastructure, ingestion configs, `dags/` folder with Python DAGs).
*   **README.md:** Technical documentation including:
    *   Data architecture diagram.
    *   Step-by-step instructions to set up the environment and run the pipeline.
    *   Explanation of the chosen ingestion strategy.
*   **Video (3-5 minutes):** Technical presentation demonstrating:
    *   Environment deployment process.
    *   Airflow UI showing successful DAG execution.
    *   Verification of data arrival at the destination.