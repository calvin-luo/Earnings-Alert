import yfinance as yf
import pandas as pd
import json
import os
import time
from datetime import datetime, timedelta
import calendar
import logging
import traceback

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("data/earnings_alert.log")
    ]
)

logger = logging.getLogger('earnings_alert')
logging.getLogger('yfinance').setLevel(logging.WARNING)

SP100_TICKERS = [
    "AAPL", "ABBV", "ABT", "ACN", "ADBE", "AIG", "ALL", "AMGN", "AMT", "AMZN", 
    "AXP", "BA", "BAC", "BIIB", "BK", "BKNG", "BLK", "BMY", "BRKB", "C", 
    "CAT", "CHTR", "CL", "CMCSA", "COF", "COP", "COST", "CRM", "CSCO", "CVS", 
    "CVX", "DD", "DHR", "DIS", "DOW", "DUK", "EMR", "EXC", "F", "FB", 
    "FDX", "GD", "GE", "GILD", "GM", "GOOG", "GOOGL", "GS", "HD", "HON", 
    "IBM", "INTC", "JNJ", "JPM", "KHC", "KMI", "KO", "LLY", "LMT", "LOW", 
    "MA", "MCD", "MDLZ", "MDT", "MET", "MMM", "MO", "MRK", "MS", "MSFT", 
    "NEE", "NFLX", "NKE", "NVDA", "ORCL", "OXY", "PEP", "PFE", "PG", "PM", 
    "PYPL", "QCOM", "RTX", "SBUX", "SO", "SPG", "T", "TGT", "TMO", "TXN", 
    "UNH", "UNP", "UPS", "USB", "V", "VZ", "WBA", "WFC", "WMT", "XOM"
]

# loads config from json file
def load_config():
    try:
        with open('config/alert_rules.json', 'r') as f:
            rules = json.load(f)
    except FileNotFoundError:
        logger.error("alert rules file not found!")
        rules = {"days_before_alert": 7}
    
    return rules

# gets dates for the current month
def get_current_month_dates(days_ahead=0):
    today = datetime.now()
    first_day = today.replace(day=1)
    
    last_day = today.replace(day=calendar.monthrange(today.year, today.month)[1])
    
    if days_ahead > 0:
        last_day = last_day + timedelta(days=days_ahead)
    
    return first_day, last_day

# tries a bunch of methods to get earnings date
def get_earnings_date_for_ticker(ticker):
    try:
        stock = yf.Ticker(ticker)
        
        try:
            calendar_data = stock.calendar
            if calendar_data is not None and 'Earnings Date' in calendar_data:
                earnings_date_obj = calendar_data['Earnings Date']
                
                if hasattr(earnings_date_obj, 'date'):
                    return earnings_date_obj.date()
                elif isinstance(earnings_date_obj, list) and len(earnings_date_obj) > 0:
                    first_date = earnings_date_obj[0]
                    if hasattr(first_date, 'date'):
                        return first_date.date()
                    elif isinstance(first_date, (datetime, pd.Timestamp)):
                        return first_date.date()
                    else:
                        return first_date
        except Exception as e:
            logger.debug(f"method 1 failed for {ticker}: {str(e)}")
        
        try:
            if hasattr(stock, 'earnings_dates') and not stock.earnings_dates.empty:
                upcoming_dates = stock.earnings_dates[stock.earnings_dates.index > datetime.now() - timedelta(days=1)]
                if not upcoming_dates.empty:
                    return upcoming_dates.index[0].date()
        except Exception as e:
            logger.debug(f"method 2 failed for {ticker}: {str(e)}")
        
        try:
            earnings_dates_df = stock.get_earnings_dates()
            if not earnings_dates_df.empty:
                upcoming_dates = earnings_dates_df[earnings_dates_df.index > datetime.now() - timedelta(days=1)]
                if not upcoming_dates.empty:
                    return upcoming_dates.index[0].date()
        except Exception as e:
            logger.debug(f"method 3 failed for {ticker}: {str(e)}")
        
        logger.info(f"couldn't find earnings date for {ticker} using any method")
        return None
    
    except Exception as e:
        logger.error(f"error processing ticker {ticker}: {str(e)}")
        return None

# gets company info and makes the name look better
def fetch_company_info(ticker):
    try:
        stock = yf.Ticker(ticker)
        info = stock.info
        
        original_name = info.get('shortName', ticker)
        
        company_name = format_company_name(original_name, ticker)
        
        company_info = {
            'name': company_name,
            'market_cap': info.get('marketCap', 'N/A'),
            'pe_ratio': info.get('trailingPE', 'N/A'),
            'sector': info.get('sector', 'N/A'),
            'industry': info.get('industry', 'N/A')
        }
        
        return company_info
    except Exception as e:
        logger.error(f"error fetching info for {ticker}: {str(e)}")
        return {
            'name': format_company_name(ticker, ticker),
            'market_cap': 'N/A',
            'pe_ratio': 'N/A',
            'sector': 'N/A',
            'industry': 'N/A'
        }

# removes corporate bs from company names
def format_company_name(name, ticker):
    name_mappings = {
        "GS": "Goldman Sachs",
        "BK": "BNY Mellon",
        "JPM": "JPMorgan Chase",
        "BAC": "Bank of America",
        "MS": "Morgan Stanley",
        "WFC": "Wells Fargo",
        "AMGN": "Amgen",
        "AAPL": "Apple",
        "MSFT": "Microsoft",
        "GOOGL": "Google",
        "GOOG": "Google",
        "AMZN": "Amazon",
        "META": "Meta",
        "FB": "Meta",
        "JNJ": "Johnson & Johnson",
        "PG": "Procter & Gamble",
        "V": "Visa",
        "MA": "Mastercard",
        "WMT": "Walmart",
        "INTC": "Intel",
        "CSCO": "Cisco",
        "VZ": "Verizon",
        "T": "AT&T",
        "PFE": "Pfizer",
        "MRK": "Merck",
        "ABBV": "AbbVie",
        "KO": "Coca-Cola",
        "PEP": "PepsiCo",
        "CVX": "Chevron",
        "XOM": "Exxon Mobil",
        "HD": "Home Depot",
        "NFLX": "Netflix",
        "NVDA": "NVIDIA",
        "ADBE": "Adobe",
        "COST": "Costco",
        "DIS": "Disney",
        "CRM": "Salesforce",
        "ABT": "Abbott",
        "TMO": "Thermo Fisher",
        "DHR": "Danaher",
        "AVGO": "Broadcom",
        "PYPL": "PayPal",
        "ACN": "Accenture",
        "BMY": "Bristol Myers Squibb",
        "LLY": "Eli Lilly",
        "UNH": "UnitedHealth",
        "MDT": "Medtronic",
        "TXN": "Texas Instruments",
        "QCOM": "Qualcomm",
        "NEE": "NextEra Energy",
        "RTX": "Raytheon",
        "HON": "Honeywell",
        "UNP": "Union Pacific",
        "LMT": "Lockheed Martin",
        "AMT": "American Tower",
        "IBM": "IBM",
        "CAT": "Caterpillar",
        "GE": "General Electric",
        "MMM": "3M",
        "AXP": "American Express",
        "BA": "Boeing",
        "LOW": "Lowe's",
        "SBUX": "Starbucks",
        "GILD": "Gilead Sciences",
        "MCD": "McDonald's",
        "SPG": "Simon Property Group",
        "CVS": "CVS Health",
        "COF": "Capital One",
        "C": "Citigroup",
        "USB": "US Bancorp",
        "TGT": "Target",
        "MO": "Altria",
        "PM": "Philip Morris",
        "BLK": "BlackRock",
        "F": "Ford",
        "GM": "General Motors",
        "MDLZ": "Mondelez",
        "BKNG": "Booking Holdings",
        "FDX": "FedEx",
        "UPS": "UPS",
        "SO": "Southern Company",
        "DUK": "Duke Energy",
        "AIG": "AIG",
        "ALL": "Allstate",
        "MET": "MetLife"
    }
    
    if ticker in name_mappings:
        return name_mappings[ticker]
    
    for suffix in [", Inc.", " Inc.", ", Corporation", " Corporation", 
                  " Corp.", ", Corp.", " Co.", ", Co.",
                  " Group, Inc. (The)", " Group", 
                  " Incorporated", ", Ltd.", " Ltd.",
                  " PLC", " Limited", " Holding", "Holdings", 
                  " International", " Intl", " Enterprises",
                  ", LLC", " LLC"]:
        if name.endswith(suffix):
            name = name[:-len(suffix)]
    
    name = name.replace("(The) ", "").replace(" (The)", "")
    
    if name.startswith("The "):
        name = name[4:]
    
    if "Bank of" in name and "America" not in name:
        parts = name.split()
        bank_index = parts.index("Bank")
        if bank_index < len(parts) - 2:
            if len(parts) > 5:
                name = " ".join(parts[:bank_index])
            
    return name.strip()

# gets earnings dates for next 30 days
def fetch_earnings_data(tickers, rules):
    batch_size = rules.get('batch_size', 20)
    look_ahead_days = rules.get('look_ahead_days', 30)
    
    today = datetime.now().date()
    end_date = today + timedelta(days=look_ahead_days)
    
    logger.info(f"fetching earnings data for {len(tickers)} tickers from {today.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
    
    earnings_data = []
    processed_tickers = set()
    
    for i in range(0, len(tickers), batch_size):
        batch = tickers[i:i+batch_size]
        logger.info(f"processing batch {i//batch_size + 1}/{(len(tickers)-1)//batch_size + 1} ({len(batch)} tickers)")
        
        for ticker in batch:
            if ticker in processed_tickers:
                logger.info(f"skipping duplicate ticker: {ticker}")
                continue
                
            processed_tickers.add(ticker)
            
            logger.info(f"processing ticker: {ticker}")
            
            earnings_date = get_earnings_date_for_ticker(ticker)
            
            if earnings_date is None:
                continue
            
            if today <= earnings_date <= end_date:
                company_info = fetch_company_info(ticker)
                
                entry = {
                    'ticker': ticker,
                    'earnings_date': earnings_date,
                    **company_info
                }
                
                earnings_data.append(entry)
                logger.info(f"added earnings entry for {ticker} on {earnings_date}")
            else:
                logger.info(f"earnings date {earnings_date} for {ticker} is outside our date range")
        
        if i + batch_size < len(tickers):
            logger.info("taking a short break between batches...")
            time.sleep(2)
    
    return earnings_data

# save earnings data to csv file
def update_earnings_history(earnings_data):
    if not earnings_data:
        logger.info("no new earnings data to add to history")
        return
    
    new_df = pd.DataFrame(earnings_data)
    
    new_df['earnings_date'] = new_df['earnings_date'].astype(str)
    
    try:
        history_df = pd.read_csv('data/earnings_history.csv')
        if 'earnings_date' in history_df.columns:
            history_df['earnings_date'] = history_df['earnings_date'].astype(str)
        
        combined_df = pd.concat([history_df, new_df]).drop_duplicates(subset=['ticker', 'earnings_date'])
    except FileNotFoundError:
        combined_df = new_df
    
    combined_df.to_csv('data/earnings_history.csv', index=False)
    logger.info(f"updated earnings history with {len(new_df)} new entries")

# create header and call table generator
def generate_markdown_table(earnings_data, rules):
    if not earnings_data:
        return "no upcoming earnings calls found."
    
    display_options = rules.get('display_options', {})
    
    sorted_data = sorted(earnings_data, key=lambda x: x['earnings_date'])
    
    today = datetime.now()
    start_date = today.strftime("%Y-%m-%d")
    end_date = (today + timedelta(days=rules.get('look_ahead_days', 30))).strftime("%Y-%m-%d")
    
    markdown = f"# S&P 100 Upcoming Earnings Calls\n\n"
    markdown += f"*Last updated: {today.strftime('%Y-%m-%d')}*\n\n"
    markdown += f"This table shows upcoming earnings calls for S&P 100 companies from {start_date} to {end_date}.\n\n"
    
    markdown += generate_consolidated_table(sorted_data, display_options)
    
    return markdown

# make the actual table
def generate_consolidated_table(earnings_data, display_options):
    headers = ["Date", "Ticker", "Company", "Industry"]
    if display_options.get('show_market_cap', True):
        headers.append("Market Cap")
    
    result = ""
    
    result += "| " + " | ".join(headers) + " |\n"
    
    separator_row = []
    for _ in headers:
        separator_row.append("---")
    result += "| " + " | ".join(separator_row) + " |\n"
    
    for entry in earnings_data:
        date_str = entry['earnings_date'].strftime("%Y-%m-%d") if hasattr(entry['earnings_date'], 'strftime') else str(entry['earnings_date'])
        market_cap_str = f"${entry['market_cap'] / 1e9:.2f}B" if isinstance(entry['market_cap'], (int, float)) else entry['market_cap']
        
        row = [
            date_str,
            entry['ticker'],
            entry['name'],
            entry['sector']
        ]
        
        if display_options.get('show_market_cap', True):
            row.append(market_cap_str)
        
        result += "| " + " | ".join(row) + " |\n"
    
    return result

# save the markdown to file
def update_alerts_markdown(markdown_content):
    with open('alerts.md', 'w') as f:
        f.write(markdown_content)
    logger.info("updated alerts.md with new earnings data")

# keep track of what we've done
def log_update(earnings_data):
    update_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    if not earnings_data:
        data = {
            'update_time': update_time,
            'num_earnings': 0,
            'earliest_date': 'None',
            'latest_date': 'None'
        }
    else:
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
        
        valid_dates = [d for d in earnings_dates if d is not None]
        
        data = {
            'update_time': update_time,
            'num_earnings': len(earnings_data),
            'earliest_date': min(valid_dates).strftime("%Y-%m-%d") if valid_dates else 'None',
            'latest_date': max(valid_dates).strftime("%Y-%m-%d") if valid_dates else 'None'
        }
    
    log_df = pd.DataFrame([data])
    
    try:
        existing_log = pd.read_csv('data/alerts_log.csv')
        updated_log = pd.concat([existing_log, log_df])
    except FileNotFoundError:
        updated_log = log_df
    
    updated_log.to_csv('data/alerts_log.csv', index=False)
    logger.info(f"logged update at {update_time}")

# main function to run everything
def main():
    logger.info("starting earnings alert update")
    
    os.makedirs('data', exist_ok=True)
    os.makedirs('config', exist_ok=True)
    
    rules = load_config()
    
    tickers = SP100_TICKERS
    
    earnings_data = fetch_earnings_data(tickers, rules)
    
    if not earnings_data:
        logger.info("no upcoming earnings calls found")
        update_alerts_markdown("no upcoming earnings calls found.")
        log_update([])
        return
    
    update_earnings_history(earnings_data)
    
    markdown = generate_markdown_table(earnings_data, rules)
    
    update_alerts_markdown(markdown)
    
    log_update(earnings_data)
    
    logger.info(f"completed earnings alert update. found {len(earnings_data)} upcoming earnings calls.")

if __name__ == "__main__":
    main()