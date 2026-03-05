#!/bin/bash
# parse_commit_range.sh - Parse git commit range input and output JSON array of commit hashes
#
# Usage: ./parse_commit_range.sh <commit-range>
#
# Examples:
#   ./parse_commit_range.sh HEAD~3..HEAD
#   ./parse_commit_range.sh abc123..def456
#   ./parse_commit_range.sh master..feature-branch
#   ./parse_commit_range.sh HEAD^
#   ./parse_commit_range.sh abc123
#
# Output: JSON array of commit hashes
#
# The script supports various git revision formats:
# - Single commit: HEAD, abc123
# - Range: HEAD~3..HEAD, master..feature
# - Relative: HEAD~3, HEAD^
# - Multiple ranges (space-separated): "HEAD~3..HEAD master..dev"

set -e

# Check if commit range argument is provided
if [ $# -eq 0 ]; then
    echo "Usage: $0 <commit-range>" >&2
    echo "Example: $0 HEAD~3..HEAD" >&2
    exit 1
fi

# Parse all arguments as potential commit ranges
COMMIT_RANGES="$*"

# Function to validate git object exists
validate_git_object() {
    if ! git rev-parse --verify "$1" >/dev/null 2>&1; then
        echo "Error: Invalid git reference: $1" >&2
        return 1
    fi
}

# Collect all commit hashes
COMMITS=""

# Process each commit range
for RANGE in $COMMIT_RANGES; do
    # Check if it's a range (contains ..)
    if [[ "$RANGE" == *".."* ]]; then
        # Split range into start and end
        START="${RANGE%..*}"
        END="${RANGE#*..}"

        # Validate both ends of the range
        validate_git_object "$START"
        validate_git_object "$END"

        # Get commits in range (excluding the start commit)
        RANGE_COMMITS=$(git rev-list --reverse "$START..$END" 2>/dev/null || true)
    else
        # Single commit or relative reference
        validate_git_object "$RANGE"

        # Check if it's a relative reference like HEAD~3
        if [[ "$RANGE" =~ (HEAD|[a-f0-9]+)(~[0-9]+|\^+) ]]; then
            # Get commits from the reference to HEAD
            BASE=$(git rev-parse "$RANGE")
            RANGE_COMMITS=$(git rev-list --reverse "$BASE..HEAD" 2>/dev/null || true)
            # If no commits found, it might be referring to a single commit
            if [ -z "$RANGE_COMMITS" ]; then
                RANGE_COMMITS=$(git rev-parse "$RANGE")
            fi
        else
            # Single commit hash or ref
            RANGE_COMMITS=$(git rev-parse "$RANGE")
        fi
    fi

    # Append to commits list
    if [ -n "$RANGE_COMMITS" ]; then
        if [ -n "$COMMITS" ]; then
            COMMITS="$COMMITS"$'\n'"$RANGE_COMMITS"
        else
            COMMITS="$RANGE_COMMITS"
        fi
    fi
done

# Remove duplicates while preserving order
COMMITS=$(echo "$COMMITS" | awk '!seen[$0]++')

# Check if we found any commits
if [ -z "$COMMITS" ]; then
    echo "Error: No commits found for range: $COMMIT_RANGES" >&2
    exit 1
fi

# Convert to JSON array
echo -n '['
FIRST=true
while IFS= read -r commit; do
    if [ -n "$commit" ]; then
        if [ "$FIRST" = true ]; then
            FIRST=false
        else
            echo -n ','
        fi
        echo -n "\"$commit\""
    fi
done <<< "$COMMITS"
echo ']'