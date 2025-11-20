# Dockerfile Runner Fix Summary

## Original Issue
The GitHub Actions self-hosted runner pods were failing to start due to a **Docker API version mismatch** between the runner container and the `docker:dind` (Docker-in-Docker) sidecar container. The base image `summerwind/actions-runner:latest` had an outdated Docker CLI that was incompatible with the newer `docker:dind` container.

## Solution Overview
Updated the Dockerfile to install the latest Docker CLI and modernize the Python environment.

## Changes Made to Dockerfile

### 1. Docker CLI Update (Lines 3-7)
**Purpose:** Fix Docker API version mismatch
```dockerfile
# Update Docker CLI to fix API version mismatch with docker:dind
RUN curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /usr/share/keyrings/docker-archive-keyring.gpg \
&& echo "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/docker-archive-keyring.gpg] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null \
&& sudo apt-get update \
&& sudo apt-get install -y docker-ce-cli \
&& sudo rm -rf /var/lib/apt/lists/*
```

### 2. Python Version Update (Lines 23-25, 31)
**Issue:** Originally using Python 3.9, but needed to balance compatibility
- Python 3.12 (Ubuntu Noble default): Too new for `dvc==2.24.0` (requires <3.11)
- Python 3.9: Too old, missing from Noble repositories and has compatibility issues

**Solution:** Use Python 3.10
- New enough to work with modern packages
- Old enough to support `dvc==2.24.0` (requires Python <3.11)
- Still receives security updates until October 2026

```dockerfile
python3.10 \
python3.10-dev \
python3.10-distutils \
```

### 3. Pip Installation Fix (Line 31)
**Issue:** System `python3-pip` was for Python 3.12, not Python 3.10

**Solution:** Use official get-pip.py for Python 3.10
```dockerfile
RUN curl -sS https://bootstrap.pypa.io/get-pip.py | python3.10
```

### 4. Pip Version Downgrade (Line 70)
**Issue:** `dvc==2.24.0` has invalid metadata that newer pip versions reject

**Solution:** Use pip <24.1 as suggested by error message
```dockerfile
RUN python3.10 -m pip install --break-system-packages 'pip<24.1' setuptools wheel
```

### 5. Make 4.3 Installation Fixes (Lines 55, 57, 64)
**Issues:**
- `ADD` command downloaded file as root, causing permission denied on extraction
- `sudo tar` created root-owned files, causing configure script to fail

**Solutions:**
- Added `--chown=runner:runner` to ADD command
- Removed `sudo` from tar extraction
- Let configure/make run as regular user, only use sudo for install

```dockerfile
ADD --chown=runner:runner http://ftp.gnu.org/gnu/make/make-4.3.tar.gz /tmp/
RUN cd /tmp \
&& tar -xzf make-4.3.tar.gz \
...
```

### 6. Update Alternatives (Line 68)
Set Python 3.10 as the default python3:
```dockerfile
RUN sudo update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.10 1
```

## Build Errors Encountered (In Order)

1. **Permission denied on configure** → Fixed by removing sudo from tar
2. **Permission denied on tar extraction** → Fixed by adding --chown to ADD
3. **No module named pip** → Fixed by installing pip for Python 3.10
4. **No module named pip3** → Changed from `python3 -m pip3` to `python3 -m pip`
5. **Externally-managed-environment (PEP 668)** → Added --break-system-packages flag
6. **numpy build failure (Python 3.12 incompatible)** → Switched to Python 3.10
7. **dvc==2.24.0 requires Python <3.11** → Python 3.10 satisfies this
8. **dvc metadata error** → Downgraded to pip<24.1
9. **Package build failures (wrong setuptools)** → Used get-pip.py for Python 3.10

## Current Status

### ✅ Completed
- Dockerfile successfully builds locally
- All Python packages install correctly
- Docker image pushed to GCR: `gcr.io/excellent-zoo-300106/ghrunner:e93a3b3b53fb`

### ❌ Current Issue: Platform Mismatch
**Problem:** Image was built on Apple Silicon (arm64/M1/M2 Mac) but GKE nodes run amd64/x86_64

**Error:**
```
Failed to pull image "gcr.io/excellent-zoo-300106/ghrunner:e93a3b3b53fb":
rpc error: code = NotFound desc = failed to pull and unpack image:
no match for platform in manifest: not found
```

**Pod Status:** ImagePullBackOff in namespace `cdi-runner`

### 🔧 Next Steps

**Rebuild for correct platform:**
```bash
cd /Users/esmelee/LLEAD-4-25/processing
docker buildx build --platform linux/amd64 -t gcr.io/excellent-zoo-300106/ghrunner:e93a3b3b53fb --push .
```

This will rebuild the image for amd64 architecture and push it to GCR, allowing the GKE pods to pull and start successfully.

## Branch Information
- Branch: `el-dockerfile-runner-fix`
- Base: `main`
- Files changed: `Dockerfile` only

## Key Learnings

1. **Platform awareness:** When building Docker images on Apple Silicon for cloud deployment, always specify `--platform linux/amd64`
2. **Python version constraints:** Check dependency requirements (like dvc<3.11) before choosing Python version
3. **Pip installation:** For non-default Python versions, use get-pip.py instead of system packages
4. **Docker permissions:** In Dockerfiles, avoid unnecessary sudo that can cause permission issues for regular user operations
5. **PEP 668:** Python 3.11+ has externally-managed-environment protection; use --break-system-packages in containers

## Files Modified
- `/Users/esmelee/LLEAD-4-25/processing/Dockerfile`
- `/Users/esmelee/LLEAD-4-25/processing/requirements.txt` (temporarily, then reverted)
