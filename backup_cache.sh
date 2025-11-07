#!/bin/bash

# Load configuration from backup.env file
ENV_FILE="$(dirname "$0")/backup.env"
if [ ! -f "$ENV_FILE" ]; then
    echo "Error: backup.env file not found at $ENV_FILE"
    echo ""
    echo "Please create backup.env file with the following variables:"
    echo "  RESTIC_REPOSITORY=s3:https://fsn1.your-objectstorage.com/rpc-cache"
    echo "  AWS_ACCESS_KEY_ID=your_access_key"
    echo "  AWS_SECRET_ACCESS_KEY=your_secret_key"
    echo ""
    echo "You can copy backup.env.example to backup.env and fill in the values."
    exit 1
fi

set -a
source "$ENV_FILE"
set +a

# Validate required variables
if [ -z "$RESTIC_REPOSITORY" ]; then
    echo "Error: Required variables not set in backup.env file:"
    echo "  - RESTIC_REPOSITORY"
    exit 1
fi

# Restic configuration
export RESTIC_REPOSITORY
export RESTIC_PASSWORD="no_password_needed"

# Backup directory (relative to script location)
BACKUP_DIR="$(dirname "$0")/rpc_cache"

# Function to initialize repository (run once)
init_repo() {
    echo "Initializing restic repository..."
    restic init
}

# Function to create backup
backup() {
    echo "Starting backup of $BACKUP_DIR..."
    restic backup "$BACKUP_DIR" \
        --tag rpc-cache \
        --verbose

    if [ $? -eq 0 ]; then
        echo "Backup completed successfully!"
    else
        echo "Backup failed!"
        exit 1
    fi
}

# Function to list snapshots
list_snapshots() {
    echo "Listing snapshots..."
    restic snapshots
}

# Function to check repository
check_repo() {
    echo "Checking repository integrity..."
    restic check
}

# Function to show repository stats
stats() {
    echo "Repository statistics..."
    restic stats
}

# Function to forget old snapshots (keep last 7 daily, 4 weekly, 6 monthly)
prune_old() {
    echo "Pruning old snapshots..."
    restic forget \
        --keep-daily 7 \
        --keep-weekly 4 \
        --keep-monthly 6 \
        --prune
}

# Main command handling
case "${1:-backup}" in
    init)
        init_repo
        ;;
    backup)
        backup
        ;;
    list)
        list_snapshots
        ;;
    check)
        check_repo
        ;;
    stats)
        stats
        ;;
    prune)
        prune_old
        ;;
    *)
        echo "Usage: $0 {init|backup|list|check|stats|prune}"
        echo ""
        echo "Commands:"
        echo "  init   - Initialize the restic repository (run once)"
        echo "  backup - Create a new backup (default)"
        echo "  list   - List all snapshots"
        echo "  check  - Check repository integrity"
        echo "  stats  - Show repository statistics"
        echo "  prune  - Remove old snapshots (keeps 7 daily, 4 weekly, 6 monthly)"
        exit 1
        ;;
esac
