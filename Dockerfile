FROM apache/superset:latest

#Switch to root to gain system-wide installation privileges
USER root

#Force pip to install globally, ignoring any user-space defaults
RUN pip install --no-cache-dir --break-system-packages trino

#Switch back to the standard superset user for execution
USER superset