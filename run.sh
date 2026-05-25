#!/bin/bash
# Auto-restart wrapper for OTP bot
# Usage: bash run.sh

cd "$(dirname "$0")"

while true; do
    echo "[*] Starting bot..."
    python3 bot.py
    EXIT_CODE=$?
    echo "[!] Bot exited (code=$EXIT_CODE), restarting in 3s..."
    sleep 3
done
