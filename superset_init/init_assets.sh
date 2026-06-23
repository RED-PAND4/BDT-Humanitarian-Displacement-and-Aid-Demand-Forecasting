#!/bin/bash

echo "Checking for exported dashboards..."
if [ -f "/configs/dashboard.zip" ]; then
  echo "Importing pre-configured dashboard..."
  # Import the dashboard as the admin user
  superset import-dashboards -p /configs/dashboard.zip -u admin
else
  echo "No dashboard.zip found to import."
fi