#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Download index minute-level data using MktBarRTIntraDayGet
Downloads data day by day based on trading calendar
"""

import argparse
import pandas as pd
from datetime import datetime
import os
import sys
import time

def get_trading_calendar(start_date, end_date):
    """
    Get trading calendar for the specified date range
    
    Parameters:
    -----------
    start_date : str
        Start date in YYYY-MM-DD format
    end_date : str
        End date in YYYY-MM-DD format
    
    Returns:
    --------
    list
        List of trading dates
    """
    from uqer import DataAPI
    
    print(f"Getting trading calendar from {start_date} to {end_date}...")
    
    # Get trading calendar
    df = DataAPI.TradeCalGet(
        exchangeCD='XSHG',
        beginDate=start_date,
        endDate=end_date,
        field='',
        pandas='1'
    )
    
    # Filter for trading days only
    trading_days = df[df['isOpen'] == 1]['calendarDate'].tolist()
    
    print(f"Found {len(trading_days)} trading days")
    return trading_days

def download_minute_data_for_day(security_id, date, unit):
    """
    Download minute data for a single day using SHSZBarHistOneDay2Get
    
    Parameters:
    -----------
    security_id : str
        Security ID (e.g., '399300.XSHE')
    date : str
        Date in YYYY-MM-DD format
    unit : int
        K-line unit in minutes (1, 5, 15, 30, 60)
    
    Returns:
    --------
    pd.DataFrame
        Minute data for the day
    """
    from uqer import DataAPI
    
    try:
        # Extract ticker from security_id and determine market
        ticker = security_id.split('.')[0]
        
        # Determine market based on ticker
        if ticker.startswith('6'):
            market = 'XSHG'  # Shanghai Stock Exchange
        else:
            market = 'XSHE'  # Shenzhen Stock Exchange
        
        # Convert date format from YYYY-MM-DD to YYYYMMDD
        date_str = date.replace('-', '')
        
        # Call SHSZBarHistOneDay2Get API
        df = DataAPI.SHSZBarHistOneDay2Get(
            date_str, market, ticker, unit=str(unit), pandas='1'
        )
        
        if df is not None and len(df) > 0:
            # Add trade date column
            df['tradeDate'] = date_str
            
            # Process the data: adjust opening price and remove 09:30
            if len(df) > 0:
                # Sort by barTime to ensure correct order
                df = df.sort_values('barTime')
                
                # Check if first bar is 09:30
                if df.iloc[0]['barTime'] == '09:30':
                    # Get the open price from 09:30 bar
                    open_price_0930 = df.iloc[0]['openPrice']
                    
                    # Find the 09:31 bar (or next available bar)
                    if len(df) > 1:
                        # Update the open price of the second bar to match 09:30 open price
                        df.iloc[1, df.columns.get_loc('openPrice')] = open_price_0930
                    
                    # Remove the 09:30 bar
                    df = df.iloc[1:].reset_index(drop=True)
            
            return df
        else:
            return None
            
    except Exception as e:
        print(f"  Error downloading {date}: {e}")
        return None

def download_minute_data_range(ticker, start_date, end_date, unit=1, output_file=None):
    """
    Download minute data for a date range by iterating through trading days
    
    Parameters:
    -----------
    ticker : str
        Index/Stock code (e.g., '399300')
    start_date : str
        Start date in YYYYMMDD format
    end_date : str
        End date in YYYYMMDD format
    unit : int
        K-line unit in minutes
    output_file : str
        Output CSV file path
    
    Returns:
    --------
    pd.DataFrame
        Combined minute data
    """
    
    # Convert dates to YYYY-MM-DD format
    start_date_formatted = f"{start_date[:4]}-{start_date[4:6]}-{start_date[6:]}"
    end_date_formatted = f"{end_date[:4]}-{end_date[4:6]}-{end_date[6:]}"
    
    # Add exchange suffix if not present
    if '.' not in ticker:
        if ticker.startswith('000') or ticker.startswith('399'):
            security_id = f"{ticker}.XSHE"
        elif ticker.startswith('6'):
            security_id = f"{ticker}.XSHG"
        else:
            security_id = ticker
    else:
        security_id = ticker
    
    print("="*60)
    print("Downloading Index Minute Data Day by Day")
    print("="*60)
    print(f"Security: {security_id}")
    print(f"Period: {start_date_formatted} to {end_date_formatted}")
    print(f"Frequency: {unit}-minute")
    print("="*60)
    
    # Get trading calendar
    trading_days = get_trading_calendar(start_date_formatted, end_date_formatted)
    
    if not trading_days:
        print("No trading days found in the specified range")
        return None
    
    # Download data day by day
    all_data = []
    total_days = len(trading_days)
    
    print(f"\nDownloading data for {total_days} trading days...")
    print("-" * 60)
    
    for i, date in enumerate(trading_days, 1):
        print(f"[{i}/{total_days}] Downloading {date}...", end=' ')
        
        df = download_minute_data_for_day(security_id, date, unit)
        
        if df is not None and len(df) > 0:
            all_data.append(df)
            print(f"✓ Got {len(df)} records")
        else:
            print(f"✗ No data")
        
        # Add a small delay to avoid rate limiting
        if i < total_days:
            time.sleep(0.1)
    
    # Combine all data
    if all_data:
        combined_df = pd.concat(all_data, ignore_index=True)
        print("\n" + "="*60)
        print(f"Successfully downloaded {len(combined_df)} total records")
        print(f"Date range: {combined_df['tradeDate'].min()} to {combined_df['tradeDate'].max()}")
        print("="*60)
        
        # Save to file
        if output_file:
            combined_df.to_csv(output_file, index=False)
            print(f"\nData saved to: {output_file}")
        
        return combined_df
    else:
        print("\nNo data downloaded")
        return None

def main():
    """Main function"""
    parser = argparse.ArgumentParser(
        description='Download index/stock minute data day by day using MktBarRTIntraDayGet',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Download CSI 300 index 1-minute data for 2025
  python download_minute_data.py -c 399300 -s 20250101 -e 20251231 -u 1 -t YOUR_TOKEN
  
  # Download with custom output file
  python download_minute_data.py -c 399300 -s 20250101 -e 20251231 -u 1 -t YOUR_TOKEN -o output.csv
        """
    )
    
    parser.add_argument('-c', '--code', required=True,
                       help='Index/Stock code (e.g., 399300 for CSI 300)')
    parser.add_argument('-s', '--start', required=True,
                       help='Start date in YYYYMMDD format (e.g., 20250101)')
    parser.add_argument('-e', '--end', required=True,
                       help='End date in YYYYMMDD format (e.g., 20251231)')
    parser.add_argument('-u', '--unit', type=int, default=1,
                       choices=[1, 5, 15, 30, 60],
                       help='K-line frequency in minutes (default: 1)')
    parser.add_argument('-t', '--token', required=True,
                       help='uqer API token (required for authentication)')
    parser.add_argument('-o', '--output', 
                       help='Output CSV file path (default: auto-generated)')
    
    args = parser.parse_args()
    
    # Validate dates
    try:
        start_dt = datetime.strptime(args.start, '%Y%m%d')
        end_dt = datetime.strptime(args.end, '%Y%m%d')
        if start_dt > end_dt:
            print("ERROR: Start date must be before end date")
            sys.exit(1)
    except ValueError as e:
        print(f"ERROR: Invalid date format: {e}")
        print("Please use YYYYMMDD format (e.g., 20250101)")
        sys.exit(1)
    
    # Generate output filename if not specified
    if args.output:
        output_file = args.output
    else:
        code_clean = args.code.replace('.', '_')
        output_file = f"{code_clean}_{args.start}_{args.end}_{args.unit}min.csv"
    
    # Login once at the beginning
    from uqer import Client
    print("\nLogging in to uqer...")
    client = Client(token=args.token)
    print("Login successful!\n")
    
    # Download data
    df = download_minute_data_range(
        ticker=args.code,
        start_date=args.start,
        end_date=args.end,
        unit=args.unit,
        output_file=output_file
    )
    
    if df is not None:
        print("\nFirst 5 rows:")
        print(df.head())
        print("\n" + "="*60)
        print("Download Complete!")
        print("="*60)
    else:
        print("\nDownload failed")
        sys.exit(1)

if __name__ == "__main__":
    main()
