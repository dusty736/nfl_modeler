DEPLOY.txt — NFL Modeling (FastAPI + Dash on Google Cloud Run)

Project: nfl-modeling • Region: europe-west2 (London)
Services:
- API (FastAPI): nfl-modeler-api
- Dashboard (Dash): nfl-modeler-dashboard
Database: Cloud SQL for Postgres (instance: nfl-pg-01)

================================================================================
0) ONE-TIME SETUP
================================================================================
gcloud auth login
gcloud config set project nfl-modeling
gcloud config set run/region europe-west2
gcloud services enable \
  run.googleapis.com cloudbuild.googleapis.com artifactregistry.googleapis.com \
  secretmanager.googleapis.com sqladmin.googleapis.com

# Create Artifact Registry repo (once)
gcloud artifacts repositories create nfl \
  --repository-format=docker \
  --location=europe-west2

# Create DB secret (example; use your real password)
echo -n 'supersecret' | gcloud secrets create DB_PASS --data-file=-

================================================================================
1) CLOUD SQL SETTINGS (UNIX SOCKET)
================================================================================
Instance connection name: nfl-modeling:europe-west2:nfl-pg-01

Pass on deploy:
--add-cloudsql-instances=nfl-modeling:europe-west2:nfl-pg-01
--set-env-vars=DB_HOST=/cloudsql/nfl-modeling:europe-west2:nfl-pg-01,DB_NAME=nfl,DB_USER=nfl_app
--set-secrets=DB_PASS=DB_PASS:latest

API db.py already uses connect_args={"host": DB_HOST} for asyncpg.

================================================================================
2) BUILD & DEPLOY — API (FASTAPI)
================================================================================
TAG=$(date +%Y%m%d%H%M%S)

gcloud builds submit services/api \
  --tag europe-west2-docker.pkg.dev/nfl-modeling/nfl/nfl_modeler_api:$TAG

gcloud run deploy nfl-modeler-api \
  --image europe-west2-docker.pkg.dev/nfl-modeling/nfl/nfl_modeler_api:$TAG \
  --allow-unauthenticated \
  --add-cloudsql-instances=nfl-modeling:europe-west2:nfl-pg-01 \
  --set-env-vars=DB_HOST=/cloudsql/nfl-modeling:europe-west2:nfl-pg-01,DB_NAME=nfl,DB_USER=nfl_app \
  --set-secrets=DB_PASS=DB_PASS:latest \
  --cpu=1 --memory=512Mi --concurrency=40 --timeout=300

# Health check
API_URL=$(gcloud run services describe nfl-modeler-api --format='value(status.url)')
curl -sSf "$API_URL/health"   # -> {"status":"ok"}

================================================================================
3) BUILD & DEPLOY — DASHBOARD (DASH)
================================================================================
3.1 Code guardrails
- app.py binds to os.environ["PORT"] (Cloud Run injects it).
- helpers/standings.py must import only:
  from helpers.api_client import _get_json_resilient
- Add to requirements.txt: requests>=2.32

3.2 Build & deploy (wire to API URL)
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

(Note: The three env vars avoid legacy fallbacks like http://api:8000.)

================================================================================
4) DAY-TO-DAY COMMANDS
================================================================================
# List services and URLs
gcloud run services list --region=europe-west2 \
  --format='table(metadata.name, status.url, status.latestCreatedRevisionName)'

# Tail logs
gcloud run services logs read nfl-modeler-api --region=europe-west2 --limit=100
gcloud run services logs read nfl-modeler-dashboard --region=europe-west2 --limit=100

# Get logs for a specific revision
REV=$(gcloud run services describe nfl-modeler-dashboard --region=europe-west2 --format='value(status.latestCreatedRevisionName)')
gcloud run services logs read nfl-modeler-dashboard --region=europe-west2 --revision="$REV" --limit=200

# Roll back traffic to a known-good revision
gcloud run services update-traffic nfl-modeler-dashboard \
  --region=europe-west2 \
  --to-revisions=<GOOD_REVISION>=100

================================================================================
5) OPTIONAL HARDENING — PRIVATE API WITH ID TOKEN
================================================================================
# Remove public invoker and grant the dashboard SA
gcloud run services remove-iam-policy-binding nfl-modeler-api \
  --region=europe-west2 \
  --member=allUsers \
  --role=roles/run.invoker

DASH_SA="cr-runtime@nfl-modeling.iam.gserviceaccount.com"
gcloud run services add-iam-policy-binding nfl-modeler-api \
  --region=europe-west2 \
  --member="serviceAccount:$DASH_SA" \
  --role="roles/run.invoker"

# Dashboard change (helpers/api_client.py):
# Acquire and send an ID token (no key files needed on Cloud Run)
# --- begin snippet ---
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

# When calling:
# r = _session.get(url, params=params, headers=_auth_headers(), timeout=timeout)
# --- end snippet ---

================================================================================
6) SCALING & COLD STARTS
================================================================================
# Cap instances
gcloud run services update nfl-modeler-api --region=europe-west2 --max-instances=10
gcloud run services update nfl-modeler-dashboard --region=europe-west2 --max-instances=5

# Keep one warm instance (optional)
gcloud run services update nfl-modeler-dashboard --region=europe-west2 --min-instances=1

================================================================================
7) SECRETS & CONFIG
================================================================================
# Update secret value
echo -n 'newpassword' | gcloud secrets versions add DB_PASS --data-file=-

# Wire secrets/envs
gcloud run services update nfl-modeler-api \
  --region=europe-west2 \
  --set-secrets=DB_PASS=DB_PASS:latest

API_URL=$(gcloud run services describe nfl-modeler-api --format='value(status.url)')
gcloud run services update nfl-modeler-dashboard \
  --region=europe-west2 \
  --set-secrets=DB_PASS=DB_PASS:latest \
  --update-env-vars=API_BASE_URL=$API_URL

================================================================================
8) LOCAL DEVELOPMENT
================================================================================
# API
cd services/api
uvicorn app.main:create_app --factory --reload --port 8000

# Dashboard
cd services/dashboard
export API_BASE_URL=http://localhost:8000
python app.py

(Note: Docker-Compose hostnames like http://api:8000 are local-only; not on Cloud Run.)

================================================================================
9) TROUBLESHOOTING QUICK HITS
================================================================================
- Dashboard calls http://api:8000 or http://localhost:8000
  -> Set API_BASE_URL, API_URL, API_BASE on the service to the API’s HTTPS URL.

- ModuleNotFoundError: api_helpers on startup
  -> Ensure: from helpers.api_client import _get_json_resilient (no cascading imports).

- “Failed to start and listen on PORT=8080” during deploy
  -> Bind to os.environ['PORT'] and fix any startup exceptions.

- DB connect errors
  -> Check add-cloudsql-instances, DB_HOST=/cloudsql/PROJECT:REGION:INSTANCE, DB_PASS secret attached.

- CORS errors
  -> Not applicable for server-side requests from Dash; only relevant if moving calls into the browser.

================================================================================
10) HANDY SNIPPETS
================================================================================
# Get service URLs
gcloud run services list --region=europe-west2 --format='table(metadata.name, status.url)'

# API health
API_URL=$(gcloud run services describe nfl-modeler-api --format='value(status.url)')
curl -sSf "$API_URL/health"

NFL Modeling — Weekly Refresh Job (Successful Steps Only)
================================================================
Generated at: 2025-09-28 20:00:04 UTC
Project: nfl-modeling
Region:  europe-west2 (London)

1) Configure Cloud Run region
   Command:
     gcloud config set run/region europe-west2
   Verification:
     gcloud config get-value run/region   -> europe-west2

2) Confirm active project and account
   Commands:
     gcloud config get-value core/project  -> nfl-modeling
     gcloud config get-value core/account  -> burnhamdustin@gmail.com

3) Build the pipeline image
   Context: services/pipeline
   Command:
     gcloud builds submit services/pipeline \
       --tag europe-west2-docker.pkg.dev/nfl-modeling/nfl/pipeline:20250928203252
   Result: SUCCESS

4) Job service account + IAM (Cloud SQL client, Secret Manager accessor)
   Service Account: cr-jobs-nfl@nfl-modeling.iam.gserviceaccount.com
   Commands:
     gcloud iam service-accounts create cr-jobs-nfl \
       --display-name="Cloud Run Jobs – NFL pipeline"
     gcloud projects add-iam-policy-binding nfl-modeling \
       --member="serviceAccount:cr-jobs-nfl@nfl-modeling.iam.gserviceaccount.com" \
       --role="roles/cloudsql.client"
     gcloud projects add-iam-policy-binding nfl-modeling \
       --member="serviceAccount:cr-jobs-nfl@nfl-modeling.iam.gserviceaccount.com" \
       --role="roles/secretmanager.secretAccessor"
   Result: Roles applied

5) Create the Cloud Run Job
   Job name: nfl-weekly-refresh
   Image: europe-west2-docker.pkg.dev/nfl-modeling/nfl/pipeline:20250928203252
   Cloud SQL instance: nfl-modeling:europe-west2:nfl-pg-01 (via Unix socket)
   Command:
     gcloud run jobs create nfl-weekly-refresh \
       --image=europe-west2-docker.pkg.dev/nfl-modeling/nfl/pipeline:20250928203252 \
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
   Result: Job created successfully

6) (Optional) Describe the job (verification)
   Command:
     gcloud run jobs describe nfl-weekly-refresh --region=europe-west2 --format='yaml'

# Update Project

2. Rebuild the Docker Image

From your project's root directory, rebuild the Docker image using the gcloud builds submit command you now know so well. This process bundles all your updated code into a new container image.

Bash

gcloud builds submit . --config=cloudbuild.yaml

Note: The cloudbuild.yaml file automatically handles the build process and tags your new image with europe-west2-docker.pkg.dev/nfl-modeling/nfl/pipeline:latest, which makes the next step easy.

3. Update the Cloud Run Job

Now, tell your Cloud Run job to use the new image you just built.
Bash

gcloud run jobs update nfl-weekly-refresh \
  --region=europe-west2 \
  --image=europe-west2-docker.pkg.dev/nfl-modeling/nfl/pipeline:latest

Since the cloudbuild.yaml file always tags the latest successful build as :latest, this command will automatically pull in your most recent code changes.
