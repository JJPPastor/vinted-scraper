# Google VM Deployment Guide

## Step 1: Create Google Cloud VM

### 1.1 Access Google Cloud Console
- Go to [Google Cloud Console](https://console.cloud.google.com/)
- Create a new project or select existing one

### 1.2 Create VM Instance
```bash
# Via Console:
1. Go to Compute Engine > VM instances
2. Click "Create Instance"
3. Configure:
   - Name: vinted-scraper
   - Machine type: e2-medium (2 vCPU, 4 GB RAM)
   - Boot disk: Ubuntu 20.04 LTS or 22.04 LTS
   - Firewall: Allow HTTP/HTTPS traffic
   - Zone: Choose closest to you
```

### 1.3 Connect to VM
```bash
# Via Console:
1. Click "SSH" button next to your instance

# Via gcloud CLI:
gcloud compute ssh vinted-scraper --zone=your-zone
```

## Step 2: Set Up Repository

### 2.1 Initialize Git Repository (Local)
```bash
# On your local machine:
cd /Users/julespastor/code/Faume/scrapers
git init
git add .
git commit -m "Initial commit: Vinted scraper with VM deployment"
```

### 2.2 Create GitHub Repository
1. Go to [GitHub](https://github.com)
2. Create new repository: `vinted-scraper`
3. Push your code:
```bash
git remote add origin https://github.com/yourusername/vinted-scraper.git
git branch -M main
git push -u origin main
```

## Step 3: Deploy on VM

### 3.1 Clone Repository on VM
```bash
# On your VM:
git clone https://github.com/yourusername/vinted-scraper.git
cd vinted-scraper
```

### 3.2 Run Setup Script
```bash
# Make script executable and run:
chmod +x setup_vm.sh
./setup_vm.sh
```

### 3.3 Activate Virtual Environment
```bash
source venv/bin/activate
```

## Step 4: Configure and Run

### 4.1 Edit Brand ID
```bash
# Edit the scraper to set your target brand:
nano vinted_scraper.py
# Change: brand_id = 115 to your target brand
```

### 4.2 Test Run
```bash
python3 vinted_scraper.py
```

### 4.3 Background Execution
```bash
# Run in background:
nohup python3 vinted_scraper.py > scraper.log 2>&1 &

# Monitor progress:
tail -f scraper.log

# Check if running:
ps aux | grep vinted_scraper
```

## Step 5: Monitoring and Management

### 5.1 Check Status
```bash
# Check if scraper is running:
ps aux | grep vinted_scraper

# View recent logs:
tail -n 50 scraper.log

# Monitor real-time:
tail -f scraper.log
```

### 5.2 Stop Scraper
```bash
# Find process ID:
ps aux | grep vinted_scraper

# Kill process:
kill <process_id>
```

### 5.3 Restart Scraper
```bash
# Kill existing process and restart:
pkill -f vinted_scraper
nohup python3 vinted_scraper.py > scraper.log 2>&1 &
```

## Step 6: Data Management

### 6.1 Download Data
```bash
# From your local machine:
gcloud compute scp vinted-scraper:~/vinted-scraper/data/vinted_tests/raw_data/ ./local-data/ --zone=your-zone
```

### 6.2 Monitor Disk Space
```bash
# Check disk usage:
df -h

# Check data directory size:
du -sh data/vinted_tests/raw_data/
```

## Troubleshooting

### Common Issues

1. **Permission Denied:**
   ```bash
   chmod +x setup_vm.sh
   chmod +x vinted_scraper.py
   ```

2. **Python Package Issues:**
   ```bash
   source venv/bin/activate
   pip install --upgrade pip
   pip install -r requirements.txt
   ```

3. **Network Issues:**
   ```bash
   # Check connectivity:
   ping google.com
   curl -I https://www.vinted.fr
   ```

4. **Memory Issues:**
   ```bash
   # Check memory usage:
   free -h
   # Consider upgrading to e2-standard-2 if needed
   ```

### Useful Commands

```bash
# View system resources:
htop

# Check disk space:
df -h

# View recent logs:
tail -n 100 scraper.log

# Restart VM if needed:
sudo reboot
```

## Cost Optimization

### VM Sizing
- **e2-medium**: Good for testing (~$25/month)
- **e2-standard-2**: Better for production (~$50/month)
- **e2-standard-4**: For heavy scraping (~$100/month)

### Stopping VM
```bash
# Stop VM when not needed:
gcloud compute instances stop vinted-scraper --zone=your-zone

# Start when needed:
gcloud compute instances start vinted-scraper --zone=your-zone
```

## Security Notes

1. **Firewall**: Only allow necessary ports
2. **SSH Keys**: Use SSH keys instead of passwords
3. **Updates**: Keep system updated
4. **Monitoring**: Monitor for unusual activity

## Next Steps

1. **Scale Up**: Consider multiple VMs for different brands
2. **Automation**: Set up cron jobs for automatic restarts
3. **Backup**: Set up automated data backups
4. **Monitoring**: Implement comprehensive monitoring 