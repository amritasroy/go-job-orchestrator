# Go Job Orchestrator (Gin + Kubernetes Jobs)

A lightweight job orchestration service built with **Go**, **Gin**, and
**Kubernetes Jobs**.\
It lets you upload files, create jobs dynamically, fetch logs, and
download results --- all with a simple REST API.

------------------------------------------------------------------------

## 🚀 Features

-   **Job orchestration via REST API**
    -   Submit jobs with uploaded artifacts
    -   Track job status
    -   Fetch logs and outputs
-   **Runtimes supported**
    -   `python`, `node`, `java`, `binary` (generic executables)
-   **Artifact handling**
    -   Secure per-job bearer tokens
    -   Upload/download artifacts & results
-   **Log handling**
    -   Redirects stdout/stderr into log files
-   **Self-cleaning**
    -   TTL-based artifact reaper removes old jobs
-   **Extensible**
    -   Built on Kubernetes `client-go` and Gin

------------------------------------------------------------------------

## 📂 Repository Structure

    go-job-orchestrator/
    ├── codebase/              # Main Go source code (the orchestrator service)
    │   ├── main.go
    │   ├── go.mod
    │   └── go.sum
    │
    ├── sample-workspace/      # Example of generated job directories & outputs
    │   ├── <jobID1>/
    │   │   ├── index.json
    │   │   ├── uploaded.py
    │   │   └── results/
    │   │       ├── stdout.log
    │   │       └── stderr.log
    │   └── <jobID2>/...
    │
    ├── .gitignore
    ├── README.md              # (this file)
    └── LICENSE

### 📑 Sample Workspace

Each job produces a folder under `STORAGE_DIR`:

    <jobID>/
    ├── index.json          # stores token + file list
    ├── <uploaded files>    # user-submitted artifacts
    └── results/
        ├── stdout.log
        ├── stderr.log
        └── <custom outputs>

See [`sample-workspace/`](./sample-workspace) for examples.

------------------------------------------------------------------------

## ⚡ Quickstart

### Prerequisites

-   Go 1.21+
-   Access to a Kubernetes cluster (or local `~/.kube/config`)

### Run locally

``` bash
cd codebase
go mod tidy
go run main.go
```

Service will listen on port **8080**.

Check health:

``` bash
curl http://localhost:8080/healthz
```

------------------------------------------------------------------------

## 🐳 Run with Docker

``` bash
# Build the image
docker build -t job-orchestrator:local ./codebase

# Run container with storage mounted
docker run --rm -p 8080:8080   -e NAMESPACE=default   -e ORCH_BASE_URL=http://localhost:8080   -e STORAGE_DIR=/data   -v "$(pwd)/data:/data"   job-orchestrator:local
```

------------------------------------------------------------------------

## ⚙️ Configuration

Environment variables:

  ------------------------------------------------------------------------------------
  Variable           Default                          Description
  ------------------ -------------------------------- --------------------------------
  `NAMESPACE`        auto-detected or `default`       Kubernetes namespace for Jobs

  `ORCH_BASE_URL`    `http://job-orchestrator:8080`   Base URL for pods to reach
                                                      orchestrator

  `STORAGE_DIR`      `/data`                          Local directory for
                                                      uploads/results

  `DATA_HOST_PATH`   (empty)                          HostPath mounted at `/data` in
                                                      job pods
  ------------------------------------------------------------------------------------

------------------------------------------------------------------------

## 🔗 API Overview

### Create a Job

`POST /api/v1/jobs` (multipart form)

Fields: - `runtime`: `python` \| `node` \| `java` \| `binary`
(required) - `entrypoint`: file to run inside pod (required) - `args`:
JSON array of strings (optional) - `env`: JSON object (optional) -
`cpu`, `memory`: resource requests/limits (optional) - `files`: one or
more files (required)

Example:

``` bash
curl -X POST http://localhost:8080/api/v1/jobs   -F runtime=python   -F entrypoint=main.py   -F args='["--flag","value"]'   -F env='{"MY_VAR":"123"}'   -F files=@main.py
```

Response:

``` json
{
  "jobID": "a1b2c3...",
  "jobName": "run-xxxxxx",
  "namespace": "default"
}
```

------------------------------------------------------------------------

### List Jobs

    GET /api/v1/jobs

### Job Status

    GET /api/v1/jobs/:id

### Job Logs

    GET /api/v1/jobs/:id/logs?tail=200

### Artifacts & Outputs

-   `GET  /artifacts/:id/:filename` -- initContainers fetch uploaded
    files\
-   `PUT  /artifacts/:id/upload/:filename` -- upload results from pod\
-   `GET  /outputs/:id/:filename` -- download saved outputs

All artifact endpoints require `Authorization: Bearer <token>`.

------------------------------------------------------------------------

## 🌐 Demo Link

👉 Live demo: **https://YOUR-DEMO-LINK.example.com**\
*(replace this with your tunnel URL, VPS endpoint, or Kubernetes Ingress
host)*

------------------------------------------------------------------------

## 🔒 Security Notes

-   Each job gets a random token for artifact access
-   Consider API key middleware if exposing publicly
-   Always deploy behind HTTPS if reachable outside cluster

------------------------------------------------------------------------

## 🛠️ Development

``` bash
cd codebase
go fmt ./...
go vet ./...
go test ./...
```

------------------------------------------------------------------------

## 📜 License

MIT --- see [LICENSE](./LICENSE)
