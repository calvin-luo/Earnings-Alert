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
        
        # Format dividend yield as percentage if available
        if isinstance(additional_metrics['dividend_yield'], (int, float)):
            additional_metrics['dividend_yield'] = f"{additional_metrics['dividend_yield'] * 100:.2f}%"
        
        # Combine metrics
        company_info.update(additional_metrics)
        
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
    processed_tickers = set()  # Track processed tickers to avoid duplicates
    
    # Process tickers in batches to avoid overwhelming the API
    for i in range(0, len(tickers), batch_size):
        batch = tickers[i:i+batch_size]
        logger.info(f"Processing batch {i//batch_size + 1}/{(len(tickers)-1)//batch_size + 1} ({len(batch)} tickers)")
        
        for ticker in batch:
            # Skip if already processed
            if ticker in processed_tickers:
                logger.info(f"Skipping duplicate ticker: {ticker}")
                continue
                
            processed_tickers.add(ticker)  # Mark ticker as processed
            
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
    
    # Sort by earnings date first
    sorted_data = sorted(earnings_data, key=lambda x: x['earnings_date'])
    
    # Generate the header of the current month and next month
    current_month = datetime.now().strftime("%B %Y")
    next_month = (datetime.now() + timedelta(days=32)).strftime("%B %Y")
    
    markdown = f"# Financial Services Earnings Calendar: {current_month} - {next_month}\n\n"
    markdown += f"*Last updated: {datetime.now().strftime('%Y-%m-%d')}*\n\n"
    markdown += f"This calendar shows upcoming earnings calls for {len(earnings_data)} financial services companies.\n\n"
    
    # Generate calendar view
    markdown += generate_calendar_view(sorted_data)
    
    # Generate consolidated table view
    markdown += "\n## Earnings Calls Details\n\n"
    markdown += generate_consolidated_table(sorted_data, display_options)
    
    return markdown

def generate_calendar_view(earnings_data):
    """Generate a calendar view for the earnings calls"""
    # Get current month and year
    today = datetime.now()
    month = today.month
    year = today.year
    
    # Get the first day of current month and number of days in month
    first_day = datetime(year, month, 1)
    if month == 12:
        last_day = datetime(year + 1, 1, 1) - timedelta(days=1)
    else:
        last_day = datetime(year, month + 1, 1) - timedelta(days=1)
    
    # Start from Monday (0) to Sunday (6)
    first_weekday = first_day.weekday()
    
    # Create calendar header
    calendar_md = "## Monthly Calendar\n\n"
    calendar_md += "| Mon | Tue | Wed | Thu | Fri | Sat | Sun |\n"
    calendar_md += "|:---:|:---:|:---:|:---:|:---:|:---:|:---:|\n"
    
    # Group earnings by date
    earnings_by_date = {}
    for entry in earnings_data:
        date = entry['earnings_date']
        if hasattr(date, 'day'):
            day = date.day
            entry_month = date.month
            entry_year = date.year
        else:
            # If it's a string, parse it
            try:
                parsed_date = datetime.strptime(str(date), "%Y-%m-%d")
                day = parsed_date.day
                entry_month = parsed_date.month
                entry_year = parsed_date.year
            except:
                continue
                
        # Only include dates in current month
        if entry_month == month and entry_year == year:
            if day not in earnings_by_date:
                earnings_by_date[day] = []
            earnings_by_date[day].append(entry['ticker'])
    
    # Generate calendar rows
    calendar_day = 1
    day_in_month = 1
    
    # Start with empty cells for days before the 1st of the month
    current_row = [""] * first_weekday
    
    while day_in_month <= last_day.day:
        # Check if this day has earnings calls
        if day_in_month in earnings_by_date:
            # Limit to first 5 tickers to avoid making cells too large
            ticker_list = earnings_by_date[day_in_month][:5]
            ticker_str = ", ".join(ticker_list)
            
            # Add "..." if there are more than 5 tickers
            if len(earnings_by_date[day_in_month]) > 5:
                ticker_str += ", ..."
                
            # Bold the date if it has earnings
            cell_content = f"**{day_in_month}**<br>{ticker_str}"
        else:
            cell_content = str(day_in_month)
            
        current_row.append(cell_content)
        
        # Move to next day
        calendar_day += 1
        day_in_month += 1
        
        # Start a new row after Sunday
        if calendar_day % 7 == 1:
            calendar_md += "| " + " | ".join(current_row) + " |\n"
            current_row = []
    
    # Add empty cells for days after the end of the month
    if current_row:
        current_row.extend([""] * (7 - len(current_row)))
        calendar_md += "| " + " | ".join(current_row) + " |\n"
    
    return calendar_md

def generate_consolidated_table(earnings_data, display_options):
    """Generate a consolidated table of all earnings calls"""
    # Standard table headers
    headers = ["Date", "Ticker", "Company", "Industry"]
    if display_options.get('show_market_cap', True):
        headers.append("Market Cap")
    if display_options.get('show_pe_ratio', True):
        headers.append("P/E Ratio")
    
    result = ""
    
    # Table header row
    result += "| " + " | ".join(headers) + " |\n"
    
    # Separator row with a dash in each cell
    separator_row = []
    for _ in headers:
        separator_row.append("---")
    result += "| " + " | ".join(separator_row) + " |\n"
    
    # Table rows
    for entry in earnings_data:
        date_str = entry['earnings_date'].strftime("%Y-%m-%d") if hasattr(entry['earnings_date'], 'strftime') else str(entry['earnings_date'])
        market_cap_str = f"${entry['market_cap'] / 1e9:.2f}B" if isinstance(entry['market_cap'], (int, float)) else entry['market_cap']
        pe_str = f"{entry['pe_ratio']:.2f}" if isinstance(entry['pe_ratio'], (int, float)) else entry['pe_ratio']
        
        row = [
            date_str,
            entry['ticker'],
            entry['name'],
            entry['industry']
        ]
        
        if display_options.get('show_market_cap', True):
            row.append(market_cap_str)
        if display_options.get('show_pe_ratio', True):
            row.append(pe_str)
        
        result += "| " + " | ".join(row) + " |\n"
    
    return result

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