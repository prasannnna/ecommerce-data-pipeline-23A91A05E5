# Docker Deployment Guide

## Prerequisites
- Docker >= 20.x
- Docker Compose >= v2
- Minimum 4GB RAM

## Quick Start

### 1. Build images
```bash
docker compose build
```
### 2. Start services
```bash
docker-compose up -d
```
### 3. Verify containers
```bash
docker ps
```
### 4. Run pipeline inside container
```bash
docker compose exec pipeline python scripts/pipeline_orchestrator.py
```
### 5. Access database
```bash
psql -h localhost -U admin -d ecommerce_db
```
### 6. View logs
```bash
docker compose logs -f pipeline
```
### 7. Stop services
```bash
docker compose down
```
### 8. Cleanup (remove volumes)
```bash
docker compose down -v
```
## Troubleshooting

### Port already in use

Stop local PostgreSQL service Or change port mapping in **docker-compose.yml**

### Database not ready

Ensure depends_on with service_healthy is configured

### Container fails to start

Check logs: **docker-compose logs**

Verify environment variables

### Network issues

Use service name **postgres** (not localhost) inside containers

## Configuration
### Environment Variables

Defined in **docker-compose.yml**:

- DB_HOST=postgres

- DB_PORT=5432

- DB_NAME=ecommerce_db

### Volumes

- postgres_data → persistent database storage

- ./data → pipeline outputs

- ./logs → pipeline logs

### Resource Limits

- PostgreSQL: default

- Pipeline: default