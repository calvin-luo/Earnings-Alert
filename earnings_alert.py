import yfinance as yf
import pandas as pd
import json
import os
import time
from datetime import datetime, timedelta
import calendar
import logging
import traceback

# setup logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("data/earnings_alert.log")
        # StreamHandler is removed, so no output to console
    ]
)

logger = logging.getLogger('earnings_alert')

# set yfinance logger to WARNING to reduce noise
logging.getLogger('yfinance').setLevel(logging.WARNING)

# load tracked tickers and alert rules
def load_config():

    """Load configuration from files"""
    try:
        with open('config/tracked_tickers.txt', 'r') as f:
            tickers = []
            for line in f:
                line = line.strip()
                if line and not line.startswith('#'):
                    # Extract just the ticker symbol (before any comment)
                    ticker = line.split('#')[0].strip()
                    if ticker:
                        tickers.append(ticker)
    except FileNotFoundError:
        logger.error("Tracked tickers file not found!")
        tickers = []
    
    try:
        with open('config/alert_rules.json', 'r') as f:
            rules = json.load(f)
    except FileNotFoundError:
        logger.error("Alert rules file not found!")
        rules = {"days_before_alert": 7}
    
    return tickers, rules

# get last day of month
def get_current_month_dates(days_ahead=0):
    """Get the first and last day of the current month, with optional extension"""
    today = datetime.now()
    first_day = today.replace(day=1)
    
    last_day = today.replace(day=calendar.monthrange(today.year, today.month)[1])
    
    if days_ahead > 0:
        last_day = last_day + timedelta(days=days_ahead)
    
    return first_day, last_day

def get_earnings_date_for_ticker(ticker):
    """
    Attempt to get earnings date for a ticker using multiple methods.
    Returns earnings_date or None if not found.
    """
    try:
        stock = yf.Ticker(ticker)
        
        # Method 1: Try calendar property
        try:
            calendar_data = stock.calendar
            if calendar_data is not None and 'Earnings Date' in calendar_data:
                earnings_date_obj = calendar_data['Earnings Date']
                
                # Handle different return types
                if hasattr(earnings_date_obj, 'date'):
                    return earnings_date_obj.date()
                elif isinstance(earnings_date_obj, list) and len(earnings_date_obj) > 0:
                    # If it's a list, take the first item
                    first_date = earnings_date_obj[0]
                    if hasattr(first_date, 'date'):
                        return first_date.date()
                    elif isinstance(first_date, (datetime, pd.Timestamp)):
                        return first_date.date()
                    else:
                        # If it's already a date-like object
                        return first_date
        except Exception as e:
            logger.debug(f"Method 1 failed for {ticker}: {str(e)}")
        
        # Method 2: Try earnings_dates property
        try:
            if hasattr(stock, 'earnings_dates') and not stock.earnings_dates.empty:
                upcoming_dates = stock.earnings_dates[stock.earnings_dates.index > datetime.now() - timedelta(days=1)]
                if not upcoming_dates.empty:
                    return upcoming_dates.index[0].date()
        except Exception as e:
            logger.debug(f"Method 2 failed for {ticker}: {str(e)}")
        
        # Method 3: Try get_earnings_dates() method
        try:
            earnings_dates_df = stock.get_earnings_dates()
            if not earnings_dates_df.empty:
                upcoming_dates = earnings_dates_df[earnings_dates_df.index > datetime.now() - timedelta(days=1)]
                if not upcoming_dates.empty:
                    return upcoming_dates.index[0].date()
        except Exception as e:
            logger.debug(f"Method 3 failed for {ticker}: {str(e)}")
        
        logger.info(f"Could not find earnings date for {ticker} using any method")
        return None
    
    except Exception as e:
        logger.error(f"Error processing ticker {ticker}: {str(e)}")
        return None

def fetch_company_info(ticker):
    """Fetch company information for a ticker"""
    try:
        stock = yf.Ticker(ticker)
        info = stock.info
        
        # Get basic info
        company_info = {
            'name': info.get('shortName', ticker),
            'market_cap': info.get('marketCap', 'N/A'),
            'pe_ratio': info.get('trailingPE', 'N/A'),
            'sector': info.get('sector', 'N/A'),
            'industry': info.get('industry', 'N/A')
        }
        
        # Get additional metrics if available
        additional_metrics = {
            'forward_pe': info.get('forwardPE', 'N/A'),
            'dividend_yield': info.get('dividendYield', 'N/A'),
            'beta': info.get('beta', 'N/A'),
            '52w_high': info.get('fiftyTwoWeekHigh', 'N/A'),
            '52w_low': info.get('fiftyTwoWeekLow', 'N/A')
        }
        
        # Get financial services specific metrics
        financial_metrics = {
            'priceToBook': info.get('priceToBook', 'N/A'),
            'returnOnEquity': info.get('returnOnEquity', 'N/A'),
            'debtToEquity': info.get('debtToEquity', 'N/A'),
            'profitMargins': info.get('profitMargins', 'N/A'),
            'operatingMargins': info.get('operatingMargins', 'N/A'),
            'netIncomeToCommon': info.get('netIncomeToCommon', 'N/A'),
            'totalCash': info.get('totalCash', 'N/A'),
            'totalDebt': info.get('totalDebt', 'N/A'),
            'currentRatio': info.get('currentRatio', 'N/A'),
            'earningsGrowth': info.get('earningsGrowth', 'N/A'),
            'revenueGrowth': info.get('revenueGrowth', 'N/A')
        }
        
        # Format dividend yield as percentage if available
        if isinstance(additional_metrics['dividend_yield'], (int, float)):
            additional_metrics['dividend_yield'] = f"{additional_metrics['dividend_yield'] * 100:.2f}%"
        
        # Combine all metrics
        company_info.update(additional_metrics)
        company_info.update(financial_metrics)
        
        return company_info
    except Exception as e:
        logger.error(f"Error fetching info for {ticker}: {str(e)}")
        return {
            'name': ticker,
            'market_cap': 'N/A',
            'pe_ratio': 'N/A',
            'sector': 'N/A',
            'industry': 'N/A',
            'forward_pe': 'N/A',
            'dividend_yield': 'N/A',
            'beta': 'N/A',
            '52w_high': 'N/A',
            '52w_low': 'N/A',
            'priceToBook': 'N/A',
            'returnOnEquity': 'N/A',
            'debtToEquity': 'N/A',
            'profitMargins': 'N/A'
        }(additional_metrics)
        
        return company_info
    except Exception as e:
        logger.error(f"Error fetching info for {ticker}: {str(e)}")
        return {
            'name': ticker,
            'market_cap': 'N/A',
            'pe_ratio': 'N/A',
            'sector': 'N/A',
            'industry': 'N/A',
            'forward_pe': 'N/A',
            'dividend_yield': 'N/A',
            'beta': 'N/A',
            '52w_high': 'N/A',
            '52w_low': 'N/A'
        }

def fetch_earnings_data(tickers, rules):
    """Fetch earnings data for the list of tickers using rules from config"""
    look_ahead_days = rules.get('look_ahead_days', 30)
    batch_size = rules.get('batch_size', 20)
    
    first_day, last_day = get_current_month_dates(look_ahead_days)
    logger.info(f"Fetching earnings data for {len(tickers)} tickers from {first_day.strftime('%Y-%m-%d')} to {last_day.strftime('%Y-%m-%d')}")
    
    earnings_data = []
    
    # Process tickers in batches to avoid overwhelming the API
    for i in range(0, len(tickers), batch_size):
        batch = tickers[i:i+batch_size]
        logger.info(f"Processing batch {i//batch_size + 1}/{(len(tickers)-1)//batch_size + 1} ({len(batch)} tickers)")
        
        for ticker in batch:
            logger.info(f"Processing ticker: {ticker}")
            
            # Get earnings date
            earnings_date = get_earnings_date_for_ticker(ticker)
            
            # Skip if no earnings date found
            if earnings_date is None:
                continue
            
            # Check if the earnings date falls within our date range
            if first_day.date() <= earnings_date <= last_day.date():
                # Get additional company information
                company_info = fetch_company_info(ticker)
                
                # Combine data
                entry = {
                    'ticker': ticker,
                    'earnings_date': earnings_date,
                    **company_info
                }
                
                earnings_data.append(entry)
                logger.info(f"Added earnings entry for {ticker} on {earnings_date}")
            else:
                logger.info(f"Earnings date {earnings_date} for {ticker} is outside our date range")
        
        # Add a small delay between batches to be kind to the API
        if i + batch_size < len(tickers):
            logger.info("Taking a short break between batches...")
            time.sleep(2)
    
    return earnings_data

def update_earnings_history(earnings_data):
    """Update the earnings history CSV file"""
    # Create DataFrame from new earnings data
    if not earnings_data:
        logger.info("No new earnings data to add to history")
        return
    
    new_df = pd.DataFrame(earnings_data)
    
    # Convert earnings_date to string for storage
    new_df['earnings_date'] = new_df['earnings_date'].astype(str)
    
    # Try to load existing history
    try:
        history_df = pd.read_csv('data/earnings_history.csv')
        # Ensure earnings_date is string for comparison
        if 'earnings_date' in history_df.columns:
            history_df['earnings_date'] = history_df['earnings_date'].astype(str)
        
        # Concatenate and remove duplicates
        combined_df = pd.concat([history_df, new_df]).drop_duplicates(subset=['ticker', 'earnings_date'])
    except FileNotFoundError:
        combined_df = new_df
    
    # Save updated history
    combined_df.to_csv('data/earnings_history.csv', index=False)
    logger.info(f"Updated earnings history with {len(new_df)} new entries")

def generate_markdown_table(earnings_data, rules):
    """Generate a markdown table from the earnings data with options from rules"""
    if not earnings_data:
        return "No upcoming earnings calls this month."
    
    display_options = rules.get('display_options', {})
    group_by_sector = display_options.get('group_by_sector', False)
    group_by_date = display_options.get('group_by_date', False)
    group_by_category = display_options.get('group_by_category', False)
    
    # Sort by earnings date first
    sorted_data = sorted(earnings_data, key=lambda x: x['earnings_date'])
    
    # Generate the header of the current month and next month
    current_month = datetime.now().strftime("%B %Y")
    next_month = (datetime.now() + timedelta(days=32)).strftime("%B %Y")
    
    markdown = f"# Financial Services Earnings Calendar: {current_month} - {next_month}\n\n"
    markdown += f"*Last updated: {datetime.now().strftime('%Y-%m-%d')}*\n\n"
    markdown += f"This table shows upcoming earnings calls for {len(earnings_data)} financial services companies.\n\n"
    
    # Standard table headers
    headers = ["Date", "Ticker", "Company", "Category", "Industry"]
    if display_options.get('show_market_cap', True):
        headers.append("Market Cap")
    if display_options.get('show_pe_ratio', True):
        headers.append("P/E Ratio")
    
    # Get financial service categories
    financial_categories = rules.get('financial_services_categories', {})
    
    # Assign category to each company
    for entry in sorted_data:
        category = "Other Financial Services"
        for cat_name, tickers in financial_categories.items():
            if entry['ticker'] in tickers:
                category = cat_name
                break
        entry['category'] = category
    
    # Function to generate a table with given data
    def generate_table(data, title=None):
        result = ""
        if title:
            result += f"## {title}\n\n"
        
        # Table header row
        result += "| " + " | ".join(headers) + " |\n"
        
        # Proper separator row with a dash in each cell
        separator_row = []
        for _ in headers:
            separator_row.append("---")
        result += "| " + " | ".join(separator_row) + " |\n"
        
        # Table rows
        for entry in data:
            date_str = entry['earnings_date'].strftime("%Y-%m-%d") if hasattr(entry['earnings_date'], 'strftime') else str(entry['earnings_date'])
            market_cap_str = f"${entry['market_cap'] / 1e9:.2f}B" if isinstance(entry['market_cap'], (int, float)) else entry['market_cap']
            pe_str = f"{entry['pe_ratio']:.2f}" if isinstance(entry['pe_ratio'], (int, float)) else entry['pe_ratio']
            
            row = [
                date_str,
                entry['ticker'],
                entry['name'],
                entry['category'],
                entry['industry']
            ]
            
            if display_options.get('show_market_cap', True):
                row.append(market_cap_str)
            if display_options.get('show_pe_ratio', True):
                row.append(pe_str)
            
            result += "| " + " | ".join(row) + " |\n"
        
        result += "\n"
        return result
    
    # Generate tables based on grouping options
    if group_by_category:
        # Group by financial service category
        category_groups = {}
        for entry in sorted_data:
            category = entry['category']
            if category not in category_groups:
                category_groups[category] = []
            category_groups[category].append(entry)
        
        # Sort categories by name
        for category, entries in sorted(category_groups.items()):
            # Sort entries by date within each category
            sorted_entries = sorted(entries, key=lambda x: x['earnings_date'])
            markdown += generate_table(sorted_entries, f"{category}")
    
    elif group_by_date:
        # Group by date only
        date_groups = {}
        for entry in sorted_data:
            date_key = entry['earnings_date'].strftime("%Y-%m-%d") if hasattr(entry['earnings_date'], 'strftime') else str(entry['earnings_date'])
            if date_key not in date_groups:
                date_groups[date_key] = []
            date_groups[date_key].append(entry)
        
        for date, entries in sorted(date_groups.items()):
            markdown += generate_table(entries, f"Earnings on {date}")
    
    else:
        # No grouping
        markdown += generate_table(sorted_data)
    
    # REMOVED: Detailed company information sections
    
    return markdown

def update_alerts_markdown(markdown_content):
    """Update the alerts.md file with the new markdown content"""
    with open('alerts.md', 'w') as f:
        f.write(markdown_content)
    logger.info("Updated alerts.md with new earnings data")

def log_update(earnings_data):
    """Log this update to the alerts_log.csv"""
    update_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # Handle empty earnings data
    if not earnings_data:
        data = {
            'update_time': update_time,
            'num_earnings': 0,
            'earliest_date': 'None',
            'latest_date': 'None'
        }
    else:
        # Extract dates and convert to strings if necessary
        earnings_dates = []
        for entry in earnings_data:
            date = entry['earnings_date']
            if hasattr(date, 'strftime'):
                earnings_dates.append(date)
            elif isinstance(date, str):
                try:
                    earnings_dates.append(datetime.strptime(date, "%Y-%m-%d").date())
                except ValueError:
                    earnings_dates.append(None)
            else:
                earnings_dates.append(None)
        
        # Filter out None values
        valid_dates = [d for d in earnings_dates if d is not None]
        
        data = {
            'update_time': update_time,
            'num_earnings': len(earnings_data),
            'earliest_date': min(valid_dates).strftime("%Y-%m-%d") if valid_dates else 'None',
            'latest_date': max(valid_dates).strftime("%Y-%m-%d") if valid_dates else 'None'
        }
    
    # Create DataFrame
    log_df = pd.DataFrame([data])
    
    # Try to append to existing log
    try:
        existing_log = pd.read_csv('data/alerts_log.csv')
        updated_log = pd.concat([existing_log, log_df])
    except FileNotFoundError:
        updated_log = log_df
    
    # Save log
    updated_log.to_csv('data/alerts_log.csv', index=False)
    logger.info(f"Logged update at {update_time}")

def main():
    """Main function to run the earnings alert system"""
    logger.info("Starting earnings alert update")
    
    # Create directories if they don't exist
    os.makedirs('data', exist_ok=True)
    os.makedirs('config', exist_ok=True)
    
    # Load configuration
    tickers, rules = load_config()
    
    if not tickers:
        logger.warning("No tickers to track. Please add tickers to config/tracked_tickers.txt")
        return
    
    # Fetch earnings data
    earnings_data = fetch_earnings_data(tickers, rules)
    
    if not earnings_data:
        logger.info("No upcoming earnings calls found for the current month")
        update_alerts_markdown("No upcoming earnings calls this month.")
        log_update([])
        return
    
    # Update history
    update_earnings_history(earnings_data)
    
    # Generate markdown
    markdown = generate_markdown_table(earnings_data, rules)
    
    # Update alerts.md
    update_alerts_markdown(markdown)
    
    # Log this update
    log_update(earnings_data)
    
    logger.info(f"Completed earnings alert update. Found {len(earnings_data)} upcoming earnings calls.")

if __name__ == "__main__":
    main()