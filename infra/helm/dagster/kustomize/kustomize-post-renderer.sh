#!/bin/bash
# Post-renderer script for Helm to apply Kustomize patches
set -e

# Read Helm-rendered manifests from stdin
cat > /tmp/helm-manifests.yaml

# Save to kustomize resources
cp /tmp/helm-manifests.yaml "$(dirname "$0")/stdin.yaml"

# Run kustomize build and output to stdout
kustomize build "$(dirname "$0")"

# Cleanup
rm -f "$(dirname "$0")/stdin.yaml"
