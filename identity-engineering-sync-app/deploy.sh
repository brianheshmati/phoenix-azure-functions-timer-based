#!/bin/bash
# Usage: ./deploy.sh [APP_NAME] [RESOURCE_GROUP]
# Defaults: APP_NAME=az-ss-sync, RESOURCE_GROUP=phx-sh-automation-us-1

APP_NAME=${1:-az-ss-sync}
RG_NAME=${2:-phx-sh-automation-us-1}

echo "🔹 Ensuring runtime is Python 3.10 ..."
az functionapp config set \
  --name $APP_NAME \
  --resource-group $RG_NAME \
  --linux-fx-version "PYTHON|3.10"

if [ $? -ne 0 ]; then
  echo "❌ Failed to set runtime"
  exit 1
fi

echo "🔹 Publishing code to Azure Function App: $APP_NAME ..."
func azure functionapp publish $APP_NAME --python

if [ $? -ne 0 ]; then
  echo "❌ Publish failed"
  exit 1
fi

echo "🔹 Restarting function app to apply changes immediately ..."
az functionapp restart --name $APP_NAME --resource-group $RG_NAME

if [ $? -eq 0 ]; then
  echo "✅ Deployment complete. New code should be live now."
else
  echo "⚠️ Restart command failed — check Azure Portal manually."
fi
