# NFL Analytics Cloud Pipeline – Project Plan

This document outlines the end‑to‑end plan for building a production‑ready analytics platform for NFL data.  The goal is to create a cloud‑hosted database that feeds both a public dashboard and machine‑learning models while keeping costs manageable.  The project follows a staged approach so you can iterate on each part before moving to the next.

## 1. Data Acquisition & Local Preparation

1. **Download raw data** – Use the `nflfastr` R package to download play‑by‑play, game, season and player datasets.  Store the raw files locally or directly in a local `/data` directory.
2. **Inspect and clean** – Inspect each dataset for missing values, unusual types and inconsistent naming.  Standardise column names and convert dates and numeric fields to appropriate formats.  Document any assumptions or transformations applied.
3. **Normalize by grain** – Split the data into tables by grain: play‑by‑play (PBP, one row per play), game (one row per game), season (one row per team‑season or player‑season), and career (one row per player).  Assign primary keys (e.g. `play_id`, `game_id`, `player_id`) and foreign keys where appropriate.  Use DuckDB or pandas to perform these transformations.
4. **Export to Parquet** – Save each cleaned table as a Parquet file.  Parquet’s columnar storage reduces size and improves load times.  Keep the files under a `processed/` folder with subdirectories for each grain (e.g. `processed/pbp/pbp.parquet`).

## 2. Cloud Setup & Database Migration

1. **Project and API setup** – Create a Google Cloud project (e.g. **NFL Modeling**) and enable the BigQuery and Cloud Storage APIs.
2. **Choose a region** – Decide between a single region such as `europe‑west2` (London) or the EU multi‑region.  Regional buckets cost ~$0.023 per GB/month while multi‑region buckets cost ~$0.020 per GB/month:contentReference[oaicite:0]{index=0}.  Pick the same region for both Cloud Storage and BigQuery to avoid egress fees.
3. **Create a Cloud Storage bucket** – Name it something unique (e.g. `nfl‑modeling‑data`) and create folders for each grain.  Upload the processed Parquet files via the console, `gsutil`, or an R package such as `googleCloudStorageR`.
4. **Select your database** – Decide whether to use **BigQuery** or **Cloud SQL**:
   - **BigQuery** is serverless and cheap at your scale; storage costs $0.02 per GB/month with the first 10 GB free:contentReference[oaicite:1]{index=1}.  It is ideal for large analytical queries but isn’t designed for frequent row‑level updates.
   - **Cloud SQL** provides a familiar PostgreSQL environment but incurs an instance fee even when idle.  A small shared‑core instance (~20 GB SSD) costs about $10/month:contentReference[oaicite:2]{index=2}.  It’s best for transactional workloads that need ACID guarantees.
5. **Load data** –
   - **BigQuery**: Create a dataset (e.g. `nfl_analytics`) and load each Parquet file into a native table or create an external table pointing to the file in Cloud Storage.  Use SQL `CREATE TABLE` statements matching your normalized schema.
   - **Cloud SQL**: Spin up a PostgreSQL instance; then use the `COPY` command or a Python script with DuckDB/pandas to import the Parquet files into your tables.
6. **Maintain referential integrity** – Define primary and foreign keys across tables so that, for example, every play references an existing game and every game references an existing season.  Add indexes on frequently queried columns for performance.
7. **Monitor costs** – If using Cloud SQL, choose SSD vs HDD storage and small instance sizes to keep monthly costs low.  For BigQuery, take advantage of the free storage and query tiers:contentReference[oaicite:3]{index=3}.

## 3. Application & Dashboards

1. **Backend API** – Develop a backend service (e.g. in Python or JavaScript) that queries the database and exposes endpoints to the dashboard.  Containerize the service using Docker and deploy it to a managed compute service such as Cloud Run.
2. **Dashboard UI** – Build a front‑end dashboard using a framework like React or Vue.  The dashboard should connect to your backend or directly to BigQuery via an intermediate API layer.  Optionally, use Looker Studio for a quick, code‑free dashboard.
3. **Authentication & security** – Use service accounts to access the database.  Do not expose database credentials client‑side.  Implement user authentication if the dashboard is restricted.

## 4. Machine‑Learning Pipeline

1. **Feature engineering** – Using the stored tables, create feature sets for modelling (e.g. rolling averages, player statistics, game context).  You can do this in SQL (BigQuery) or in Python notebooks.
2. **Model training** – For lightweight models (e.g. logistic regression), consider **BigQuery ML** which allows training directly in SQL with no infrastructure to manage.  For more complex models or GPU‑accelerated training, use **Vertex AI**; a training job on an `n1‑standard‑8` instance with a T4 GPU costs ~$0.379 for the VM plus $0.35 per hour for the GPU:contentReference[oaicite:4]{index=4}.  Alternatively, train locally or in Jupyter notebooks using Vertex AI Workbench.
3. **Model deployment** – Deploy trained models via Vertex AI endpoints or incorporate them into your containerized backend.  Use Cloud Run to host prediction services.
4. **Integration** – Surface model predictions in your dashboard, such as win probabilities or player performance forecasts.

## 5. Scheduling & Automation

1. **Update cadence** – Plan for off‑season monthly updates, weekly updates during the season, and near real‑time play‑by‑play ingestion during games.
2. **Cloud Scheduler** – Use Cloud Scheduler to trigger batch ETL jobs (implemented as Cloud Run Jobs or Cloud Functions) on a schedule.  Cloud Functions’ first 2 million invocations each month are free:contentReference[oaicite:5]{index=5}.
3. **Real‑time ingestion** – For in‑game PBP data, set up Cloud Storage event notifications or Pub/Sub topics.  When a new play arrives, a Cloud Function can transform and append it to the database.
4. **Monitoring & alerting** – Set up logging and Cloud Monitoring to track job failures, query costs, and data freshness.  Configure cost alerts to avoid unexpected bills.

## 6. Next Steps

1. **Prototype locally** – Start by running the R scripts and transformation code locally, then exporting Parquet files.
2. **Implement the cloud pipeline** – Create the bucket, upload processed files, and load them into BigQuery.  Verify that the data fits the schema and run sample queries.
3. **Build the dashboard backend** – Containerize a simple API that queries your tables; deploy it to Cloud Run and test with sample endpoints.
4. **Iterate** – Once the basics work, flesh out the dashboard UI, expand the ML models, and refine the ETL jobs.  Incorporate automation for regular updates.

(Thanks ChatGPT PM)

This staged approach allows you to quickly get an MVP up and running while leaving room for future enhancements and cost optimisation.  The plan emphasises clear grain definitions, cost awareness, proper cloud setup, and modular components that can evolve over time.
