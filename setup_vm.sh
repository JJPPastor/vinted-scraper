#!/bin/bash

# Vinted Scraper VM Setup Script
# Run this script on your Google VM to set up the scraper

echo "🚀 Setting up Vinted Scraper on VM..."

# Update system
echo "📦 Updating system packages..."
sudo apt update && sudo apt upgrade -y

# Install Python and dependencies
echo "🐍 Installing Python and dependencies..."
sudo apt install -y python3 python3-pip python3-venv git curl wget

# Create virtual environment
echo "🔧 Creating Python virtual environment..."
python3 -m venv venv
source venv/bin/activate

# Install Python packages
echo "📚 Installing Python packages..."
pip install --upgrade pip
pip install -r requirements.txt

# Create data directory
echo "📁 Creating data directory..."
mkdir -p ../data/vinted_tests/raw_data

# Set permissions
echo "🔐 Setting permissions..."
chmod +x vinted_scraper.py

echo "✅ Setup complete!"
echo ""
echo "To run the scraper:"
echo "1. Activate virtual environment: source venv/bin/activate"
echo "2. Run scraper: python3 vinted_scraper.py"
echo ""
echo "For background execution:"
echo "nohup python3 vinted_scraper.py > scraper.log 2>&1 &"
echo ""
echo "To monitor progress:"
echo "tail -f scraper.log" 