FROM python:3.12-slim

WORKDIR /app

# Only runtime tools needed in final image
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt /app/requirements.txt
COPY pandas_ta-0.4.67b0.tar.gz /app/pandas_ta-0.4.67b0.tar.gz
RUN pip install --upgrade pip setuptools wheel \
    && pip install --no-cache-dir /app/pandas_ta-0.4.67b0.tar.gz \
    && pip install --no-cache-dir -r /app/requirements.txt

# Copy application code
COPY . /app

EXPOSE 8501

HEALTHCHECK CMD curl --fail http://localhost:8501/_stcore/health || exit 1

CMD ["streamlit", "run", "streamlit_app.py", "--server.port=8501", "--server.address=0.0.0.0"]
