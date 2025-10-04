Here's the content you provided, reorganized into a clear and well-formatted Markdown document. I've also integrated the prediction-related commands from your previous code discussions into a new "ML Model Deployment" section, as that's a key part of the workflow.

-----

# 🏈 NFL Modeling on Google Cloud Run: Deployment Guide

This document outlines the end-to-end deployment process for the NFL modeling project, which includes a FastAPI backend, a Dash dashboard, and automated data pipelines.

**Project**: `nfl-modeling`
**Region**: `europe-west2` (London)
**Services**:

  - **API (FastAPI)**: `nfl-modeler-api`
  - **Dashboard (Dash)**: `nfl-modeler-dashboard`
  - **Database**: Cloud SQL for PostgreSQL (`nfl-pg-01`)

-----

## 1\. One-Time Setup 🛠️

These steps are required only once per Google Cloud project.

### 1.1 Authenticate and Configure

Log in to your Google Cloud account and set the project and region for all future commands.

```bash
gcloud auth login
gcloud config set project nfl-modeling
gcloud config set run/region europe-west2
```

### 1.2 Enable APIs & Create Artifact Registry

Enable the necessary Google Cloud APIs and create a Docker repository to store your container images.

```bash
gcloud services enable \
  run.googleapis.com cloudbuild.googleapis.com artifactregistry.googleapis.com \
  secretmanager.googleapis.com sqladmin.googleapis.com

# Create Artifact Registry repo (once)
gcloud artifacts repositories create nfl \
  --repository-format=docker \
  --location=europe-west2
```

### 1.3 Create Database Secret

Store your database password in Secret Manager for secure access from Cloud Run.

```bash
echo -n 'supersecret' | gcloud secrets create DB_PASS --data-file=-
```

-----

## 2\. Cloud SQL Settings (UNIX Socket) 🔒

Cloud Run uses a secure, private connection to Cloud SQL via a UNIX socket. This is the **most secure and recommended** method. The `DB_HOST` is a special path, not a network address.

**Instance Connection Name**: `nfl-modeling:europe-west2:nfl-pg-01`

When deploying services, pass the following arguments:

```bash
# Connection string passed to gcloud run deploy
--add-cloudsql-instances=nfl-modeling:europe-west2:nfl-pg-01 \
--set-env-vars=DB_HOST=/cloudsql/nfl-modeling:europe-west2:nfl-pg-01,DB_NAME=nfl,DB_USER=nfl_app \
--set-secrets=DB_PASS=DB_PASS:latest
```

The API's database connection code (`db.py`) must be configured to use this host path.

-----

## 3\. Deploy the API (FastAPI) 🚀

This service provides the data to the dashboard and other clients.

### 3.1 Build the Docker Image

Build the API image using Cloud Build and tag it with a timestamp.

```bash
TAG=$(date +%Y%m%d%H%M%S)

gcloud builds submit services/api \
  --tag europe-west2-docker.pkg.dev/nfl-modeling/nfl/nfl_modeler_api:$TAG
```

### 3.2 Deploy to Cloud Run

Deploy the image as a new Cloud Run service.

```bash
TAG=$(date +%Y%m%d%H%M%S)

gcloud run deploy nfl-modeler-api \
  --image europe-west2-docker.pkg.dev/nfl-modeling/nfl/nfl_modeler_api:$TAG \
  --allow-unauthenticated \
  --add-cloudsql-instances=nfl-modeling:europe-west2:nfl-pg-01 \
  --set-env-vars=DB_HOST=/cloudsql/nfl-modeling:europe-west2:nfl-pg-01,DB_NAME=nfl,DB_USER=nfl_app \
  --set-secrets=DB_PASS=DB_PASS:latest \
  --cpu=1 --memory=512Mi --concurrency=40 --timeout=300
```

### 3.3 Verify Deployment

After deployment, check the API's health endpoint.

```bash
API_URL=$(gcloud run services describe nfl-modeler-api --format='value(status.url)')
curl -sSf "$API_URL/health"
# Expected output: {"status":"ok"}
```

### 3.4 Connect to Database

Running the following will allow for connection and updating of the database hosted on GC.

```bash
./cloud-sql-proxy --unix-socket /tmp nfl-modeling:europe-west2:nfl-pg-01
```

-----

## 4\. Deploy the Dashboard (Dash) 📊

The dashboard is a separate Cloud Run service that consumes data from the API.

### 4.1 Build and Deploy

Build the dashboard's Docker image and deploy it, linking it to the API service URL.

```bash
TAG=$(date +%Y%m%d%H%M%S)
API_URL=$(gcloud run services describe nfl-modeler-api --format='value(status.url)')

gcloud builds submit services/dashboard \
  --tag europe-west2-docker.pkg.dev/nfl-modeling/nfl/nfl_modeler_dashboard:$TAG

gcloud run deploy nfl-modeler-dashboard \
  --image europe-west2-docker.pkg.dev/nfl-modeling/nfl/nfl_modeler_dashboard:$TAG \
  --add-cloudsql-instances=nfl-modeling:europe-west2:nfl-pg-01 \
  --set-secrets=DB_PASS=DB_PASS:latest \
  --update-env-vars=API_BASE_URL=$API_URL,API_URL=$API_URL,API_BASE=$API_URL \
  --cpu=1 --memory=512Mi --concurrency=40 --timeout=300
```

-----

## 5\. ML Model Deployment (Cloud Run Jobs) 🤖

This section covers the one-time setup and day-to-day commands for running your prediction scripts on Cloud Run Jobs. The scripts are executed within a containerized pipeline.

### 5.1 Create Job Service Account & IAM

Create a dedicated service account for your jobs and grant it the necessary IAM roles for Cloud SQL and Secret Manager.

```bash
gcloud iam service-accounts create cr-jobs-nfl \
   --display-name="Cloud Run Jobs – NFL pipeline"

gcloud projects add-iam-policy-binding nfl-modeling \
   --member="serviceAccount:cr-jobs-nfl@nfl-modeling.iam.gserviceaccount.com" \
   --role="roles/cloudsql.client"
   
gcloud projects add-iam-policy-binding nfl-modeling \
   --member="serviceAccount:cr-jobs-nfl@nfl-modeling.iam.gserviceaccount.com" \
   --role="roles/secretmanager.secretAccessor"
```

### 5.2 Create the Cloud Run Job

Create the Cloud Run job that will run your weekly prediction pipeline.

```bash
# Build the pipeline image first, as outlined in your provided script
gcloud builds submit services/pipeline \
   --tag europe-west2-docker.pkg.dev/nfl-modeling/nfl/pipeline:latest

# Create the job
gcloud run jobs create nfl-weekly-refresh \
   --image=europe-west2-docker.pkg.dev/nfl-modeling/nfl/pipeline:latest \
   --region=europe-west2 \
   --tasks=1 \
   --max-retries=0 \
   --service-account=cr-jobs-nfl@nfl-modeling.iam.gserviceaccount.com \
   --set-cloudsql-instances=nfl-modeling:europe-west2:nfl-pg-01 \
   --set-env-vars=DB_HOST=/cloudsql/nfl-modeling:europe-west2:nfl-pg-01,DB_NAME=nfl,DB_USER=nfl_app \
   --set-secrets=DB_PASS=DB_PASS:latest \
   --cpu=2 \
   --memory=2Gi \
   --task-timeout=3600s
```

### 5.3 Trigger a Manual Job Execution

To run the job outside of a schedule, use the `execute` command.

```bash
gcloud run jobs execute nfl-weekly-refresh --region=europe-west2
```

### 5.4 Update the Job After Code Changes

When you update your Python prediction scripts, rebuild the Docker image and update the Cloud Run Job to use the latest version.

```bash
# Rebuild the Docker image and tag as :latest
gcloud builds submit . --config=cloudbuild.yaml

# Update the job to use the new :latest image
gcloud run jobs update nfl-weekly-refresh \
  --region=europe-west2 \
  --image=europe-west2-docker.pkg.dev/nfl-modeling/nfl/pipeline:latest
```

-----

## 6\. Day-to-Day Commands 📝

Useful commands for managing your services and jobs.

### 6.1 Services

```bash
# List services and URLs
gcloud run services list --region=europe-west2 \
  --format='table(metadata.name, status.url, status.latestCreatedRevisionName)'

# Tail logs for a specific service
gcloud run services logs read nfl-modeler-api --region=europe-west2 --limit=100
gcloud run services logs read nfl-modeler-dashboard --region=europe-west2 --limit=100
```

### 6.2 Jobs

```bash
# List job executions
gcloud run jobs list-executions nfl-weekly-refresh --region=europe-west2

# Tail logs for a specific job execution
gcloud run jobs executions logs nfl-weekly-refresh --region=europe-west2
```

-----

## 7\. Optional Hardening: Private API with ID Token 🔐

To prevent public access to your API, you can restrict access to only your dashboard's service account.

### 7.1 Remove Public Access & Grant Permissions

Remove the `allUsers` role and grant `roles/run.invoker` to the dashboard's service account.

```bash
gcloud run services remove-iam-policy-binding nfl-modeler-api \
  --region=europe-west2 \
  --member=allUsers \
  --role=roles/run.invoker

DASH_SA="cr-runtime@nfl-modeling.iam.gserviceaccount.com"
gcloud run services add-iam-policy-binding nfl-modeler-api \
  --region=europe-west2 \
  --member="serviceAccount:$DASH_SA" \
  --role="roles/run.invoker"
```

### 7.2 Update Dashboard Code

Your dashboard's code must now acquire and send an ID token with each API request.

```python
# In helpers/api_client.py
import os, requests
from google.auth.transport.requests import Request
from google.oauth2 import id_token

_session = requests.Session()
_req = Request()
_AUD = (os.getenv("API_BASE_URL") or os.getenv("API_URL") or os.getenv("API_BASE") or "").rstrip("/")

def _auth_headers():
    try:
        tok = id_token.fetch_id_token(_req, _AUD)
        return {"Authorization": f"Bearer {tok}"}
    except Exception:
        return {}

# When making a request:
# r = _session.get(url, params=params, headers=_auth_headers(), timeout=timeout)
```

-----

## 8\. Scaling & Cold Starts 📈

Manage service performance and responsiveness.

```bash
# Set max instances to control cost
gcloud run services update nfl-modeler-api --region=europe-west2 --max-instances=10
gcloud run services update nfl-modeler-dashboard --region=europe-west2 --max-instances=5

# Keep one warm instance to reduce cold start latency
gcloud run services update nfl-modeler-dashboard --region=europe-west2 --min-instances=1
```

-----

## 9\. Local Development 💻

Run your services locally for testing before deployment.

### 9.1 Run the API

```bash
cd services/api
uvicorn app.main:create_app --factory --reload --port 8000
```

### 9.2 Run the Dashboard

```bash
cd services/dashboard
export API_BASE_URL=http://localhost:8000
python app.py
```
