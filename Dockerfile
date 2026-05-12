FROM python:3.11-slim

# Install system dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends gcc && \
    rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Install uv
RUN pip install --upgrade pip && \
    pip install uv

# Copy requirements file
COPY requirements.txt .

# Install dependencies using uv with --system flag
RUN uv pip install --system -r requirements.txt

# Copy the rest of the application
COPY . .

EXPOSE 8000

# Run the streamable-HTTP MCP server directly (serves /mcp on port 8000)
CMD ["python", "-m", "meta_ads_mcp"]
