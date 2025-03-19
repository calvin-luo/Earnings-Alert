"""
Test script for earnings_alert.py
Uses a much smaller subset of tickers to test functionality quickly
"""
import os
import logging
from earnings_alert import (
    get_earnings_date_for_ticker,
    fetch_company_info,
    update_alerts_markdown,
    generate_markdown_table,
    generate_calendar_view,
    generate_consolidated_table
)

# Setup basic logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('test_earnings_alert')

# Sample config with a few tickers from different categories
test_config = {
    "display_options": {
        "show_pe_ratio": True,
        "show_market_cap": True
    }
}

def run_test():
    """Run a small test with a subset of tickers"""
    logger.info("Starting test with small ticker subset")
    
    # Create test directories if needed
    os.makedirs('data', exist_ok=True)
    
    # Test tickers (just a few)
    test_tickers = ['JPM', 'GS', 'BLK', 'V', 'MA', 'MET', 'PYPL']
    
    # Collect earnings data
    earnings_data = []
    
    for ticker in test_tickers:
        logger.info(f"Processing {ticker}")
        
        # Get earnings date
        earnings_date = get_earnings_date_for_ticker(ticker)
        
        if earnings_date:
            logger.info(f"Found earnings date for {ticker}: {earnings_date}")
            
            # Get company info
            company_info = fetch_company_info(ticker)
            
            # Add to earnings data
            earnings_data.append({
                'ticker': ticker,
                'earnings_date': earnings_date,
                **company_info
            })
        else:
            logger.warning(f"No earnings date found for {ticker}")
    
    if earnings_data:
        # Generate markdown
        markdown = generate_markdown_table(earnings_data, test_config)
        
        # Also test the individual components
        calendar_view = generate_calendar_view(earnings_data)
        table_view = generate_consolidated_table(earnings_data, test_config['display_options'])
        
        # Save to test file
        with open('test_alerts.md', 'w') as f:
            f.write(markdown)
            
        logger.info(f"Test completed successfully. Found {len(earnings_data)} earnings dates.")
        logger.info("Results saved to test_alerts.md")
    else:
        logger.error("No earnings data found for any test tickers")

if __name__ == "__main__":
    run_test()