# Financial Services Earnings Alert

Python tool that automatically tracks and displays upcoming earnings calls for select financial services firms.

## Project Structure

```
EARNINGS-ALERT/
├── .github/workflows/      # GitHub Actions automation
├── alerts.md               # The generated earnings calendar (main output)
├── config/                 # Configuration files
│   ├── alert_rules.json    # Display preferences and filtering rules
│   └── tracked_tickers.txt # List of companies to track
├── data/                   # Data storage
│   ├── alerts_log.csv      # Log of updates
│   └── earnings_history.csv # Historical earnings data
└── earnings_alert.py       # Main Python script
```

## License

This project is licensed under the MIT License

## Acknowledgments

Data provided via yfinance (Yahoo)
