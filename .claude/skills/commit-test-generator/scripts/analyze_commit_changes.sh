#!/bin/bash
# analyze_commit_changes.sh - Extract Java files and changed classes/methods from a commit
#
# Usage: ./analyze_commit_changes.sh <commit-hash>
#
# Example:
#   ./analyze_commit_changes.sh abc123def
#
# Output: JSON with file paths and changed classes/methods
# {
#   "commit": "abc123def",
#   "java_files": [
#     {
#       "path": "src/main/java/org/example/MyClass.java",
#       "is_test": false,
#       "module": "flink-core",
#       "classes": ["MyClass"],
#       "methods": ["doSomething", "processData"]
#     }
#   ],
#   "test_files": [
#     {
#       "path": "src/test/java/org/example/MyClassTest.java",
#       "is_test": true,
#       "module": "flink-core"
#     }
#   ]
# }

set -e

# Check if commit hash argument is provided
if [ $# -eq 0 ]; then
    echo "Usage: $0 <commit-hash>" >&2
    echo "Example: $0 abc123def" >&2
    exit 1
fi

COMMIT_HASH="$1"

# Validate commit exists
if ! git rev-parse --verify "$COMMIT_HASH" >/dev/null 2>&1; then
    echo "Error: Invalid commit hash: $COMMIT_HASH" >&2
    exit 1
fi

# Get list of changed Java files in the commit
CHANGED_FILES=$(git diff-tree --no-commit-id --name-only -r "$COMMIT_HASH" | grep "\.java$" || true)

# Initialize JSON output
echo -n '{'
echo -n "\"commit\":\"$COMMIT_HASH\","
echo -n '"java_files":['

FIRST_FILE=true
TEST_FILES=""

# Process each changed Java file
while IFS= read -r file; do
    # Skip if file doesn't exist in the commit (might be deleted)
    if ! git show "$COMMIT_HASH:$file" >/dev/null 2>&1; then
        continue
    fi

    # Determine if it's a test file
    IS_TEST=false
    if [[ "$file" == *"/test/"* ]] || [[ "$file" == *"Test.java" ]] || [[ "$file" == *"ITCase.java" ]]; then
        IS_TEST=true
        # Store test files for separate section
        if [ -n "$TEST_FILES" ]; then
            TEST_FILES="$TEST_FILES|$file"
        else
            TEST_FILES="$file"
        fi
        continue
    fi

    # Extract module name from path (e.g., flink-core from flink-core/src/main/java/...)
    MODULE=""
    if [[ "$file" =~ ^([^/]+)/src/ ]]; then
        MODULE="${BASH_REMATCH[1]}"
    fi

    # Get the file content at the commit
    FILE_CONTENT=$(git show "$COMMIT_HASH:$file" 2>/dev/null || echo "")

    # Extract class names (public, abstract, interface, enum)
    CLASSES=$(echo "$FILE_CONTENT" | grep -E "^[[:space:]]*(public|protected|private)?[[:space:]]*(static)?[[:space:]]*(final)?[[:space:]]*(abstract)?[[:space:]]*(class|interface|enum)[[:space:]]+" | \
              sed -E 's/.*\b(class|interface|enum)[[:space:]]+([A-Za-z0-9_]+).*/\2/' | \
              sort -u | tr '\n' ',' | sed 's/,$//')

    # Extract changed methods by analyzing the diff
    # Get the diff hunks for this file
    DIFF_CONTENT=$(git diff "$COMMIT_HASH^" "$COMMIT_HASH" -- "$file" 2>/dev/null || echo "")

    # Extract method names from added or modified lines in the diff
    # Look for method signatures (public/private/protected followed by return type and method name)
    METHODS=$(echo "$DIFF_CONTENT" | grep "^[+]" | \
              grep -E "(public|protected|private|static|final|synchronized|native|abstract)[[:space:]]+" | \
              grep -E "\b[A-Za-z0-9_<>,\[\]]+[[:space:]]+[a-z][A-Za-z0-9_]*[[:space:]]*\(" | \
              sed -E 's/.*\b([a-z][A-Za-z0-9_]*)[[:space:]]*\(.*/\1/' | \
              grep -v "^return\|^throw\|^new\|^if\|^while\|^for\|^switch\|^try\|^catch" | \
              sort -u | tr '\n' ',' | sed 's/,$//')

    # Add to JSON output
    if [ "$FIRST_FILE" = false ]; then
        echo -n ','
    else
        FIRST_FILE=false
    fi

    echo -n '{'
    echo -n "\"path\":\"$file\","
    echo -n '"is_test":false,'
    echo -n "\"module\":\"$MODULE\","

    # Add classes array
    echo -n '"classes":['
    if [ -n "$CLASSES" ]; then
        IFS=',' read -ra CLASS_ARRAY <<< "$CLASSES"
        FIRST_CLASS=true
        for class in "${CLASS_ARRAY[@]}"; do
            if [ -n "$class" ]; then
                if [ "$FIRST_CLASS" = false ]; then
                    echo -n ','
                else
                    FIRST_CLASS=false
                fi
                echo -n "\"$class\""
            fi
        done
    fi
    echo -n '],'

    # Add methods array
    echo -n '"methods":['
    if [ -n "$METHODS" ]; then
        IFS=',' read -ra METHOD_ARRAY <<< "$METHODS"
        FIRST_METHOD=true
        for method in "${METHOD_ARRAY[@]}"; do
            if [ -n "$method" ]; then
                if [ "$FIRST_METHOD" = false ]; then
                    echo -n ','
                else
                    FIRST_METHOD=false
                fi
                echo -n "\"$method\""
            fi
        done
    fi
    echo -n ']'

    echo -n '}'
done <<< "$CHANGED_FILES"

echo -n '],'

# Add test files section
echo -n '"test_files":['

if [ -n "$TEST_FILES" ]; then
    IFS='|' read -ra TEST_ARRAY <<< "$TEST_FILES"
    FIRST_TEST=true
    for test_file in "${TEST_ARRAY[@]}"; do
        if [ -n "$test_file" ]; then
            # Extract module name
            MODULE=""
            if [[ "$test_file" =~ ^([^/]+)/src/ ]]; then
                MODULE="${BASH_REMATCH[1]}"
            fi

            if [ "$FIRST_TEST" = false ]; then
                echo -n ','
            else
                FIRST_TEST=false
            fi

            echo -n '{'
            echo -n "\"path\":\"$test_file\","
            echo -n '"is_test":true,'
            echo -n "\"module\":\"$MODULE\""
            echo -n '}'
        fi
    done
fi

echo -n ']'
echo '}'