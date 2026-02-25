# Docker Deployment Guide

## 📋 Overview

This guide covers deploying the application with simplified session-based authentication in Docker.

**Note:** This guide has been updated to reflect the removal of the cookie manager. Authentication now uses Streamlit's built-in session state, which is simpler and more reliable.

## 🚀 Quick Rebuild (No Changes Needed)

Your **existing Dockerfile works fine**. Simply rebuild:

```bash
# Build the image
docker build -t streampage:latest .

# Run the container
docker run -p 8501:8501 streampage:latest
```

That's it! The new authentication features will work automatically.

## 📝 What Gets Included Automatically

When you build the Docker image, these new files are automatically included:

✅ `common_utils/auth.py` - Updated authentication with cookies  
✅ `.streamlit/config.toml` - Session configuration  
✅ `requirements.txt` - Including `streamlit-cookies-manager`  
✅ All application code and dependencies  

## 🔐 Production Deployment (Recommended)

### Step 1: Update Configuration Secrets

Before deploying to production, update the session secret in `.streamlit/config.toml`:

```toml
# Generate a secure random secret
cookieSecret = "your_generated_secure_secret_here"
```

**Generate a secure secret:**

```bash
# Generate random secret for Streamlit's session cookie
python -c "import secrets; print(secrets.token_hex(32))"
```

**Note:** This is Streamlit's built-in session cookie (not the removed cookie manager). It's used for CSRF protection and session management.

### Step 2: Create Environment File (Optional)

If you need to customize database settings:

```bash
cp env.example .env
```

Edit `.env` with your database configuration:

```env
DB_HOST=your_db_host
DB_PORT=5432
DB_USER=trading_user
DB_PASSWORD=your_secure_password
DB_NAME=nsedata
DB_TRADING=trading_db
```

⚠️ **Never commit `.env` to git!** (Already in .gitignore)

### Step 3: Build and Run with Docker Compose

```bash
# Build the image
docker-compose -f docker-compose.production.yml build

# Run the container
docker-compose -f docker-compose.production.yml up -d

# Check logs
docker-compose -f docker-compose.production.yml logs -f
```

## 🛠️ Build Options

### Option 1: Using Existing Dockerfile (Simplest)

```bash
# Build
docker build -t streampage:latest .

# Run with environment variables
docker run -d \
  -p 8501:8501 \
  -e COOKIE_SECRET_KEY=your_secret \
  -e STREAMLIT_COOKIE_SECRET=your_secret \
  -v $(pwd)/logs:/app/logs \
  --name streampage \
  streampage:latest
```

### Option 2: Using Production Dockerfile (Recommended)

```bash
# Build
docker build -f Dockerfile.production -t streampage:production .

# Run
docker run -d \
  -p 8501:8501 \
  --env-file .env \
  -v $(pwd)/logs:/app/logs \
  --name streampage \
  streampage:production
```

### Option 3: Using Docker Compose (Best for Production)

```bash
# One command to build and run
docker-compose -f docker-compose.production.yml up -d
```

## 📦 Updating the Application

When you make changes to the code:

```bash
# Rebuild the image
docker-compose -f docker-compose.production.yml build --no-cache

# Restart the container
docker-compose -f docker-compose.production.yml up -d

# Or do both in one command
docker-compose -f docker-compose.production.yml up -d --build
```

## 🔍 Verification

### 1. Check Container is Running

```bash
docker ps | grep streampage
```

### 2. Check Logs

```bash
# Docker compose
docker-compose -f docker-compose.production.yml logs -f

# Or with docker directly
docker logs -f streampage
```

### 3. Check Health

```bash
docker inspect --format='{{.State.Health.Status}}' streampage
```

### 4. Test Authentication

1. Open browser: `http://localhost:8501`
2. Login with "Remember me" checked
3. Refresh the page (F5)
4. ✅ Should stay logged in

## 🐛 Troubleshooting

### Issue: Authentication cookies not working in Docker

**Solution**: Ensure the cookie secrets are set properly:

```bash
# Check environment variables
docker exec streampage env | grep COOKIE
```

### Issue: Session still timing out

**Possible causes**:
1. Cookie secrets not properly set
2. Browser blocking cookies
3. HTTPS not configured (required for secure cookies in production)

**Solutions**:
```bash
# Check Streamlit config is loaded
docker exec streampage cat /app/.streamlit/config.toml

# Check auth.py has correct cookie settings
docker exec streampage grep -A 5 "COOKIE_EXPIRY_DAYS" /app/common_utils/auth.py
```

### Issue: Can't access database from container

**Solution**: Update `config.ini` or use environment variables:

```bash
docker run -d \
  -p 8501:8501 \
  -e DB_HOST=host.docker.internal \
  streampage:latest
```

For Linux, use: `--add-host=host.docker.internal:host-gateway`

## 📊 Production Checklist

Before deploying to production:

- [ ] Generate strong random secrets for cookies
- [ ] Update secrets in environment variables (not hardcoded)
- [ ] Enable HTTPS/SSL (required for secure cookies)
- [ ] Set up proper database connection
- [ ] Configure volume mounts for logs
- [ ] Set up monitoring and health checks
- [ ] Test authentication persistence
- [ ] Review and update `.streamlit/config.toml` settings
- [ ] Set appropriate `COOKIE_EXPIRY_DAYS` value
- [ ] Configure backup strategy
- [ ] Set up proper logging

## 🌐 HTTPS Configuration (Important!)

For production, cookies should be served over HTTPS. Add to your `docker-compose.production.yml`:

```yaml
services:
  nginx:
    image: nginx:latest
    ports:
      - "443:443"
    volumes:
      - ./nginx.conf:/etc/nginx/nginx.conf
      - ./ssl:/etc/nginx/ssl
    depends_on:
      - streampage
```

## 📈 Monitoring

### Check Authentication Logs

```bash
# View authentication events
docker exec streampage tail -f /app/logs/streampage.log | grep -i auth

# Check for login events
docker exec streampage grep "User logged in" /app/logs/streampage.log

# Check for session restoration
docker exec streampage grep "Session restored from cookies" /app/logs/streampage.log
```

### Health Check Endpoint

Streamlit provides a health endpoint:
```bash
curl http://localhost:8501/_stcore/health
```

## 🔄 CI/CD Integration

### Example GitHub Actions Workflow

```yaml
name: Build and Deploy

on:
  push:
    branches: [ main ]

jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      
      - name: Build Docker image
        run: docker build -t streampage:${{ github.sha }} .
      
      - name: Run tests
        run: docker run streampage:${{ github.sha }} pytest
      
      - name: Push to registry
        run: |
          docker tag streampage:${{ github.sha }} your-registry/streampage:latest
          docker push your-registry/streampage:latest
```

## 📝 Summary

### For Development:
```bash
docker build -t streampage:latest . && docker run -p 8501:8501 streampage:latest
```

### For Production:
```bash
# Set up environment
cp env.example .env
# Edit .env with secure values

# Deploy
docker-compose -f docker-compose.production.yml up -d
```

### To Update:
```bash
docker-compose -f docker-compose.production.yml up -d --build
```

---

**That's it! Your Docker deployment now supports persistent authentication.** 🎉

