#!/bin/bash
# Hypesilico Validation Script
#
# Orchestrates the validation process:
# 1. Sets default environment variables
# 2. Builds the project (unless --skip-build)
# 3. Starts the server if not already running
# 4. Waits for health endpoint
# 5. Runs Python validation script
# 6. Reports results and cleans up
#
# Usage:
#   ./scripts/validate.sh [OPTIONS]
#
# Options:
#   --skip-build      Skip cargo build step
#   --keep-running    Don't stop server after validation
#   --help            Show this help message

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# Configuration
PORT="${PORT:-8080}"
BASE_URL="http://localhost:${PORT}"
HEALTH_TIMEOUT=30
HEALTH_INTERVAL=1

# Default environment variables (only set if not already defined)
export DATABASE_PATH="${DATABASE_PATH:-/tmp/hypesilico_validation.db}"
export HYPERLIQUID_API_URL="${HYPERLIQUID_API_URL:-https://api.hyperliquid.xyz}"
export TARGET_BUILDER="${TARGET_BUILDER:-0x0000000000000000000000000000000000000000}"
export BUILDER_ATTRIBUTION_MODE="${BUILDER_ATTRIBUTION_MODE:-auto}"
export PNL_MODE="${PNL_MODE:-gross}"
export PORT="${PORT}"

# Parse arguments
SKIP_BUILD=false
KEEP_RUNNING=false

show_help() {
    echo "Hypesilico Validation Script"
    echo ""
    echo "Usage: ./scripts/validate.sh [OPTIONS]"
    echo ""
    echo "Options:"
    echo "  --skip-build      Skip cargo build step"
    echo "  --keep-running    Don't stop server after validation"
    echo "  --help            Show this help message"
    echo ""
    echo "Environment Variables:"
    echo "  PORT              Server port (default: 8080)"
    echo "  DATABASE_PATH     SQLite database path"
    echo "  HYPERLIQUID_API_URL  Hyperliquid API URL"
    echo "  TARGET_BUILDER    Builder address for attribution"
    echo "  PNL_MODE          gross or net (default: gross)"
}

while [[ $# -gt 0 ]]; do
    case $1 in
        --skip-build)
            SKIP_BUILD=true
            shift
            ;;
        --keep-running)
            KEEP_RUNNING=true
            shift
            ;;
        --help|-h)
            show_help
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            echo "Use --help for usage information"
            exit 1
            ;;
    esac
done

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

log_step() {
    echo -e "${BLUE}[STEP]${NC} $1"
}

# Track if we started the server
SERVER_PID=""

# Cleanup function
cleanup() {
    if [[ -n "$SERVER_PID" ]] && [[ "$KEEP_RUNNING" != "true" ]]; then
        log_info "Stopping server (PID: $SERVER_PID)"
        kill "$SERVER_PID" 2>/dev/null || true
        wait "$SERVER_PID" 2>/dev/null || true
    elif [[ -n "$SERVER_PID" ]] && [[ "$KEEP_RUNNING" == "true" ]]; then
        log_info "Server left running (PID: $SERVER_PID)"
    fi
}
trap cleanup EXIT

# Check if server is already running
check_server_running() {
    local response
    response=$(curl -s -o /dev/null -w "%{http_code}" "${BASE_URL}/health" 2>/dev/null)
    [[ "$response" == "200" ]]
}

# Wait for server health
wait_for_health() {
    local elapsed=0
    log_info "Waiting for server health at ${BASE_URL}/health..."

    while [[ $elapsed -lt $HEALTH_TIMEOUT ]]; do
        if check_server_running; then
            echo ""
            log_info "Server is healthy!"
            return 0
        fi
        sleep $HEALTH_INTERVAL
        elapsed=$((elapsed + HEALTH_INTERVAL))
        echo -n "."
    done

    echo ""
    log_error "Server health check timed out after ${HEALTH_TIMEOUT}s"
    return 1
}

# Main execution
cd "$PROJECT_ROOT"

echo ""
echo "======================================"
echo "  Hypesilico Validation Harness"
echo "======================================"
echo ""
log_info "Configuration:"
echo "  Database:        $DATABASE_PATH"
echo "  API URL:         $HYPERLIQUID_API_URL"
echo "  Target Builder:  ${TARGET_BUILDER:0:10}..."
echo "  Port:            $PORT"
echo "  PnL Mode:        $PNL_MODE"
echo "  Attribution:     $BUILDER_ATTRIBUTION_MODE"
echo ""

# Check if server already running
if check_server_running; then
    log_info "Server already running at ${BASE_URL}"
    SERVER_PID=""
else
    # Build if needed
    if [[ "$SKIP_BUILD" != "true" ]]; then
        log_step "Building project..."
        cargo build --release 2>&1 | tail -5
        echo ""
    else
        log_info "Skipping build (--skip-build)"
    fi

    # Check if binary exists
    if [[ ! -f "./target/release/hypesilico" ]]; then
        log_error "Binary not found at ./target/release/hypesilico"
        log_error "Run without --skip-build to build first"
        exit 1
    fi

    # Start server
    log_step "Starting server..."
    ./target/release/hypesilico &
    SERVER_PID=$!
    log_info "Server started with PID: $SERVER_PID"

    # Wait for health
    if ! wait_for_health; then
        log_error "Failed to start server"
        exit 1
    fi
fi

echo ""

# Run validation
log_step "Running validation..."
echo ""

VALIDATION_EXIT=0
python3 "${SCRIPT_DIR}/validate.py" \
    --base-url "$BASE_URL" \
    --expected "${PROJECT_ROOT}/validation/expected.json" \
    || VALIDATION_EXIT=$?

echo ""

if [[ $VALIDATION_EXIT -eq 0 ]]; then
    echo "======================================"
    log_info "Validation PASSED!"
    echo "======================================"
else
    echo "======================================"
    log_error "Validation FAILED!"
    echo "======================================"
fi

exit $VALIDATION_EXIT
