# Windows Setup Tutorial for Databricks Toolkit

**For colleagues who will inevitably say "it's broken"** 😅

## Prerequisites (Install These First)

### 1. Python 3.11+
```bash
# Download from python.org
# Make sure to check "Add Python to PATH" during installation
# Verify installation:
python --version
```

### 2. Git for Windows
```bash
# Download from git-scm.com
# Use default settings during installation
# Verify installation:
git --version
```

### 3. Google Cloud SDK
```bash
# Download from cloud.google.com/sdk
# Run the installer as Administrator
# Verify installation:
gcloud --version
```

## Step-by-Step Setup

### Step 1: Clone the Repository
```bash
# Open Command Prompt or PowerShell as Administrator
# Navigate to where you want the project
cd C:\Users\YourName\Documents\Projects

# Clone the repository
git clone https://github.com/yourusername/databricks_toolkit.git
cd databricks_toolkit
```

### Step 2: Create Virtual Environment
```bash
# Create virtual environment
python -m venv .venv

# Activate virtual environment
# For Command Prompt:
.venv\Scripts\activate

# For PowerShell:
.venv\Scripts\Activate.ps1

# You should see (.venv) at the start of your prompt
```

### Step 3: Install Dependencies
```bash
# Upgrade pip first
python -m pip install --upgrade pip

# Install requirements
pip install -r requirements.txt
```

### Step 4: Google Cloud Authentication
```bash
# Login to Google Cloud (opens browser)
gcloud auth login

# Set your project (replace with your project ID)
gcloud config set project your-project-id

# Verify authentication
gcloud auth list
```

### Step 5: Databricks Authentication
```bash
# Configure Databricks CLI
databricks configure --token

# Enter your workspace URL and token when prompted
```

## Testing the Installation

### Test 1: Basic Python
```bash
python -c "print('Python is working!')"
```

### Test 2: Google Cloud
```bash
gcloud auth list
```

### Test 3: Billing Monitor
```bash
make billing-check THRESHOLD=100
```

### Test 4: Full Pipeline
```bash
python main.py --help
```

## Common "It's Broken" Issues and Solutions

### Issue 1: "Python not found"
**Solution:**
- Reinstall Python and check "Add to PATH"
- Restart Command Prompt after installation

### Issue 2: "pip not found"
**Solution:**
```bash
python -m pip install --upgrade pip
```

### Issue 3: "gcloud not found"
**Solution:**
- Reinstall Google Cloud SDK
- Restart Command Prompt
- Run: `gcloud init`

### Issue 4: "Virtual environment not activating"
**Solution:**
```bash
# For Command Prompt:
.venv\Scripts\activate

# For PowerShell (if blocked):
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
.venv\Scripts\Activate.ps1
```

### Issue 5: "Module not found errors"
**Solution:**
```bash
# Make sure virtual environment is activated
# Reinstall requirements
pip install -r requirements.txt --force-reinstall
```

### Issue 6: "Authentication failed"
**Solution:**
```bash
# Re-authenticate
gcloud auth login
databricks configure --token
```

### Issue 7: "Make commands don't work"
**Solution:**
- Install Git Bash (comes with Git for Windows)
- Use Git Bash instead of Command Prompt
- Or use PowerShell with: `choco install make`

## Quick Commands Reference

### Billing Monitoring
```bash
# Check costs
make billing-costs YEAR=2025 MONTH=7

# Check threshold
make billing-check THRESHOLD=100



### Pipeline Commands
```bash
# Run pipeline
python main.py --input_table your_table --bronze_path bronze_table --silver_path silver_table --gold_path gold_table

# Test pipeline
python -m pytest tests/
```

### Git Bash Commands (if using Git Bash)
```bash
# Make commands work in Git Bash
make billing-costs YEAR=2025 MONTH=7
make test
make lint
```

## Troubleshooting Checklist

Before saying "it's broken", check:

- [ ] Python 3.11+ installed and in PATH
- [ ] Git installed
- [ ] Google Cloud SDK installed
- [ ] Virtual environment activated (see (.venv) in prompt)
- [ ] All dependencies installed (`pip install -r requirements.txt`)
- [ ] Google Cloud authenticated (`gcloud auth login`)
- [ ] Databricks configured (`databricks configure --token`)
- [ ] Using correct terminal (Command Prompt, PowerShell, or Git Bash)

## Getting Help

If it's still "broken":

1. **Copy the exact error message**
2. **Check the troubleshooting checklist above**
3. **Try the common solutions above**
4. **Ask for help with the specific error message**

**Remember: The toolkit works on Windows - it's just a setup issue!** 🚀
