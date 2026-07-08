#!/bin/bash
# Gradually backfill test_daily_summaries one day at a time to avoid
# impacting the production database. Each day runs as a separate pod
# with a configurable pause between days.
#
# Usage:
#   ./scripts/backfill-daily-summaries.sh [OPTIONS]
#
# Options:
#   --days N          Number of days to backfill (default: 90)
#   --namespace NS    Kubernetes namespace (default: sippy)
#   --image IMAGE     Container image (default: auto-detect from sippy DC)
#   --db-secret NAME  Database secret name (default: postgres-aws)
#   --pause SECONDS   Seconds to wait between days (default: 5)
#   --batch N         Days per pod (default: 1)
#   --dry-run         Print commands without executing

set -euo pipefail

DAYS=90
NAMESPACE=sippy
IMAGE=""
DB_SECRET=postgres-aws
PAUSE=5
BATCH=1
DRY_RUN=false

while [[ $# -gt 0 ]]; do
    case "$1" in
        --days)       DAYS="$2"; shift 2 ;;
        --namespace)  NAMESPACE="$2"; shift 2 ;;
        --image)      IMAGE="$2"; shift 2 ;;
        --db-secret)  DB_SECRET="$2"; shift 2 ;;
        --pause)      PAUSE="$2"; shift 2 ;;
        --batch)      BATCH="$2"; shift 2 ;;
        --dry-run)    DRY_RUN=true; shift ;;
        *)            echo "Unknown option: $1" >&2; exit 1 ;;
    esac
done

if [[ -z "$IMAGE" ]]; then
    IMAGE=$(oc -n "$NAMESPACE" get dc/sippy -o jsonpath='{.spec.template.spec.containers[0].image}' 2>/dev/null) || true
    if [[ -z "$IMAGE" ]]; then
        echo "Could not auto-detect image. Use --image to specify." >&2
        exit 1
    fi
    echo "Using image: $IMAGE"
fi

today=$(date -u +%Y-%m-%d)
completed=0
failed=0

for ((offset=DAYS; offset>0; offset-=BATCH)); do
    batch_end_offset=$((offset - BATCH))
    if (( batch_end_offset < 0 )); then
        batch_end_offset=0
    fi

    start_date=$(date -u -v-${offset}d +%Y-%m-%d 2>/dev/null || date -u -d "$today - ${offset} days" +%Y-%m-%d)
    end_date=$(date -u -v-${batch_end_offset}d +%Y-%m-%d 2>/dev/null || date -u -d "$today - ${batch_end_offset} days" +%Y-%m-%d)

    pod_name="sippy-backfill-$(echo "$start_date" | tr -d '-')"
    echo "[$(date +%H:%M:%S)] Processing $start_date to $end_date ($((DAYS - offset + BATCH))/$DAYS days)..."

    if [[ "$DRY_RUN" == "true" ]]; then
        echo "  Would create pod $pod_name: sippy refresh --daily-summaries-start=$start_date --daily-summaries-end=$end_date"
        completed=$((completed + 1))
        continue
    fi

    # Clean up any leftover pod from a previous run
    oc -n "$NAMESPACE" delete pod "$pod_name" --ignore-not-found --wait=false >/dev/null 2>&1 || true
    sleep 2

    oc -n "$NAMESPACE" run "$pod_name" --rm -i --restart=Never \
        --image="$IMAGE" \
        --overrides="{
            \"spec\": {
                \"containers\": [{
                    \"name\": \"$pod_name\",
                    \"image\": \"$IMAGE\",
                    \"command\": [\"sippy\"],
                    \"args\": [\"refresh\",
                        \"--daily-summaries-start=$start_date\",
                        \"--daily-summaries-end=$end_date\",
                        \"--refresh-only-if-empty\"],
                    \"env\": [{
                        \"name\": \"SIPPY_DATABASE_DSN\",
                        \"valueFrom\": {\"secretKeyRef\": {\"name\": \"$DB_SECRET\", \"key\": \"dsn\"}}
                    }]
                }]
            }
        }" \
        -- sippy refresh \
            --daily-summaries-start="$start_date" \
            --daily-summaries-end="$end_date" \
            --refresh-only-if-empty 2>&1 | grep -iE 'daily summar|error|elapsed|complete' || true

    exit_code=${PIPESTATUS[0]}
    if [[ $exit_code -eq 0 ]]; then
        completed=$((completed + 1))
        echo "  Done."
    else
        failed=$((failed + 1))
        echo "  FAILED (exit $exit_code)" >&2
    fi

    if (( offset - BATCH > 0 )); then
        sleep "$PAUSE"
    fi
done

echo ""
echo "Backfill complete: $completed succeeded, $failed failed out of $(( (DAYS + BATCH - 1) / BATCH )) batches."
