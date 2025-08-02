# Vinted Scraper

A robust web scraper for Vinted that handles anti-bot measures, proxy rotation, and automatic resumption.

## Features

- ✅ **Anti-bot bypass** using cloudscraper and fake-useragent
- ✅ **Proxy rotation** with automatic fallback to direct connection
- ✅ **Auto-resume** functionality - continues from where it left off
- ✅ **Robust error handling** with retry mechanisms
- ✅ **Data persistence** - saves progress incrementally
- ✅ **Session management** with multiple fallback strategies

## Quick Start

### 1. Install Dependencies
```bash
pip install -r requirements.txt
```

### 2. Run the Scraper
```bash
python vinted_scraper.py
```

## Configuration

### Brand ID
Edit the `brand_id` variable in `vinted_scraper.py`:
```python
brand_id = 115  # Change this to your target brand ID
```

### Proxy Configuration
The scraper automatically:
1. Tests direct connection (local IP) first
2. Falls back to proxies if needed
3. Caches working proxies for efficiency

### Auto-Resume
The scraper automatically detects the last processed category and page, allowing seamless resumption after interruptions.

## Output

Data is saved to `../data/vinted_tests/raw_data/` in CSV format:
- `{brand_id}_{page_count}.csv`

## VM Deployment

### Google Cloud VM Setup

1. **Create VM Instance:**
   ```bash
   # Choose Ubuntu 20.04 or 22.04 LTS
   # Machine type: e2-medium (2 vCPU, 4 GB RAM)
   # Allow HTTP/HTTPS traffic
   ```

2. **Connect to VM:**
   ```bash
   gcloud compute ssh your-instance-name --zone=your-zone
   ```

3. **Install Dependencies:**
   ```bash
   sudo apt update
   sudo apt install python3 python3-pip git
   ```

4. **Clone Repository:**
   ```bash
   git clone <your-repo-url>
   cd scrapers
   pip3 install -r requirements.txt
   ```

5. **Run Scraper:**
   ```bash
   python3 vinted_scraper.py
   ```

### Background Execution
```bash
# Run in background
nohup python3 vinted_scraper.py > scraper.log 2>&1 &

# Monitor progress
tail -f scraper.log

# Check if running
ps aux | grep vinted_scraper
```

## Troubleshooting

### Common Issues

1. **403 Forbidden:**
   - Scraper automatically tries proxy rotation
   - Check if your IP is blocked

2. **Connection Timeout:**
   - Scraper has built-in retry logic
   - Check network connectivity

3. **Data Parsing Errors:**
   - Fixed to handle missing photo data
   - Graceful handling of None values

### Logs
- Check `scraper.log` for detailed output
- Monitor console output for real-time progress

## File Structure

```
scrapers/
├── vinted_scraper.py          # Main scraper
├── requirements.txt           # Dependencies
├── README.md                 # This file
└── ../data/vinted_tests/raw_data/  # Output directory
```

## Performance

- **Efficient:** Caches working proxies
- **Resilient:** Multiple fallback strategies
- **Scalable:** Can run 24/7 on VM
- **Resumable:** Continues after interruptions

## License

This project is for educational purposes. Please respect Vinted's terms of service. 