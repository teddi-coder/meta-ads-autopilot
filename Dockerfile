FROM python:3.11-slim

# Install system dependencies + Node.js for supergateway
RUN apt-get update && \
    apt-get install -y --no-install-recommends gcc nodejs npm && \
    rm -rf /var/lib/apt/lists/*

# Install supergateway (wraps stdio MCP as HTTP/SSE server)
RUN npm install -g supergateway

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

# Wrap the stdio MCP server with supergateway so it binds to HTTP 0.0.0.0:8000
CMD ["supergateway", "--stdio", "python -m meta_ads_mcp", "--port", "8000", "--host", "0.0.0.0"]
