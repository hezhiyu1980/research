#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Download daily K-line data from uqer (优矿)
Supports index and stock data with optional price adjustment
"""

import argparse
import pandas as pd
from datetime import datetime
import os
import sys

def download_index_daily_data(ticker, start_date, end_date, token=None, adj_flag=None):
    """
    Download daily index data from uqer
    
    Parameters:
    -----------
    ticker : str
        Index code, e.g., '399300' for CSI 300
    start_date : str
        Start date in format 'YYYYMMDD'
    end_date : str
        End date in format 'YYYYMMDD'
    token : str
        uqer API token for authentication
    adj_flag : str, optional
        Adjustment flag: '1' for forward adjustment (前复权), '2' for backward adjustment (后复权), None for no adjustment
    
    Returns:
    --------
    pd.DataFrame
        Downloaded daily data
    """
    try:
        # Import uqer client
        from uqer import Client
        
        # Login with token
        if token:
            print(f"Logging in with provided token...")
            client = Client(token=token)
        else:
            print("ERROR: Token is required. Use -t parameter to provide your uqer token.")
            sys.exit(1)
        
        # Import DataAPI
        from uqer import DataAPI
        
        print(f"\nDownloading daily data for {ticker}...")
        print(f"  Period: {start_date} to {end_date}")
        
        # Convert date format from YYYYMMDD to YYYY-MM-DD
        start_date_formatted = f"{start_date[:4]}-{start_date[4:6]}-{start_date[6:]}"
        end_date_formatted = f"{end_date[:4]}-{end_date[4:6]}-{end_date[6:]}"
        print(f"  Formatted dates: {start_date_formatted} to {end_date_formatted}")
        
        # For index data, ticker should not have exchange suffix
        # Remove .XSHE or .XSHG if present
        if '.' in ticker:
            ticker_clean = ticker.split('.')[0]
        else:
            ticker_clean = ticker
        
        print(f"  Ticker: {ticker_clean}")
        
        # Call API to get daily data
        # MktIdxdGet is for index daily data (unit='d' for daily)
        api_params = {
            'ticker': ticker_clean,
            'beginDate': start_date_formatted,
            'endDate': end_date_formatted,
            'unit': 'd',  # 'd' for daily
            'field': '',
            'pandas': '1'
        }
        
        # Add adjustment flag if specified
        if adj_flag:
            api_params['adjustFlag'] = str(adj_flag)
            adj_type = '前复权' if adj_flag == '1' else '后复权' if adj_flag == '2' else '不复权'
            print(f"  Adjustment: {adj_type}")
        else:
            print(f"  Adjustment: 不复权")
        
        df = DataAPI.MktIdxdGet(**api_params)
        
        if df is None or len(df) == 0:
            print("WARNING: No data returned from API")
            return None
        
        print(f"\nSuccessfully downloaded {len(df)} daily records")
        
        # Check available columns and print date range
        if 'tradeDate' in df.columns:
            print(f"Date range: {df['tradeDate'].min()} to {df['tradeDate'].max()}")
        else:
            print(f"Available columns: {df.columns.tolist()}")
        
        return df
        
    except ImportError as e:
        print(f"ERROR: Failed to import uqer library: {e}")
        print("Please install uqer: pip install uqer")
        sys.exit(1)
    except Exception as e:
        print(f"ERROR: Failed to download data: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

def download_stock_daily_data(ticker, start_date, end_date, token=None, adj_flag=None):
    """
    Download daily stock data from uqer
    
    Parameters:
    -----------
    ticker : str
        Stock code, e.g., '000001.XSHE' for Ping An Bank
    start_date : str
        Start date in format 'YYYYMMDD'
    end_date : str
        End date in format 'YYYYMMDD'
    token : str
        uqer API token for authentication
    adj_flag : str, optional
        Adjustment flag: '1' for forward adjustment (前复权), '2' for backward adjustment (后复权), None for no adjustment
    
    Returns:
    --------
    pd.DataFrame
        Downloaded daily data
    """
    try:
        # Import uqer client
        from uqer import Client
        
        # Login with token
        if token:
            print(f"Logging in with provided token...")
            client = Client(token=token)
        else:
            print("ERROR: Token is required. Use -t parameter to provide your uqer token.")
            sys.exit(1)
        
        # Import DataAPI
        from uqer import DataAPI
        
        print(f"\nDownloading daily data for {ticker}...")
        print(f"  Period: {start_date} to {end_date}")
        
        # Convert date format from YYYYMMDD to YYYY-MM-DD
        start_date_formatted = f"{start_date[:4]}-{start_date[4:6]}-{start_date[6:]}"
        end_date_formatted = f"{end_date[:4]}-{end_date[4:6]}-{end_date[6:]}"
        print(f"  Formatted dates: {start_date_formatted} to {end_date_formatted}")
        
        print(f"  Ticker: {ticker}")
        
        # Call API to get daily data
        # MktEqudGet is for stock daily data (unit='d' for daily)
        api_params = {
            'ticker': ticker,
            'beginDate': start_date_formatted,
            'endDate': end_date_formatted,
            'unit': 'd',  # 'd' for daily
            'field': '',
            'pandas': '1'
        }
        
        # Add adjustment flag if specified
        if adj_flag:
            api_params['adjustFlag'] = str(adj_flag)
            adj_type = '前复权' if adj_flag == '1' else '后复权' if adj_flag == '2' else '不复权'
            print(f"  Adjustment: {adj_type}")
        else:
            print(f"  Adjustment: 不复权")
        
        df = DataAPI.MktEqudGet(**api_params)
        
        if df is None or len(df) == 0:
            print("WARNING: No data returned from API")
            return None
        
        print(f"\nSuccessfully downloaded {len(df)} daily records")
        
        # Check available columns and print date range
        if 'tradeDate' in df.columns:
            print(f"Date range: {df['tradeDate'].min()} to {df['tradeDate'].max()}")
        else:
            print(f"Available columns: {df.columns.tolist()}")
        
        return df
        
    except ImportError as e:
        print(f"ERROR: Failed to import uqer library: {e}")
        print("Please install uqer: pip install uqer")
        sys.exit(1)
    except Exception as e:
        print(f"ERROR: Failed to download data: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

def main():
    """Main function with command line interface"""
    parser = argparse.ArgumentParser(
        description='Download daily K-line data from uqer',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Download CSI 300 daily data (no adjustment)
  python download_daily_data.py -c 399300 -s 20140101 -e 20260306 -t YOUR_TOKEN -o 399300_day.csv
  
  # Download stock daily data with forward adjustment
  python download_daily_data.py -c 000001.XSHE -s 20200101 -e 20231231 -t YOUR_TOKEN --adj 1 -o stock_daily.csv
        """
    )
    
    parser.add_argument('-c', '--code', required=True,
                        help='Stock/Index code (e.g., 399300 for CSI 300, 000001.XSHE for stock)')
    parser.add_argument('-s', '--start', required=True,
                        help='Start date in YYYYMMDD format')
    parser.add_argument('-e', '--end', required=True,
                        help='End date in YYYYMMDD format')
    parser.add_argument('-t', '--token', required=True,
                        help='uqer API token')
    parser.add_argument('-o', '--output', default='daily_data.csv',
                        help='Output CSV file name (default: daily_data.csv)')
    parser.add_argument('--adj', type=int, choices=[1, 2],
                        help='Price adjustment: 1=forward (前复权), 2=backward (后复权), default=no adjustment')
    
    args = parser.parse_args()
    
    # Determine if it's an index or stock based on ticker format
    if '.' in args.code:
        print(f"Detected stock code: {args.code}")
        df = download_stock_daily_data(
            ticker=args.code,
            start_date=args.start,
            end_date=args.end,
            token=args.token,
            adj_flag=args.adj
        )
    else:
        print(f"Detected index code: {args.code}")
        df = download_index_daily_data(
            ticker=args.code,
            start_date=args.start,
            end_date=args.end,
            token=args.token,
            adj_flag=args.adj
        )
    
    if df is not None and len(df) > 0:
        # Save to CSV
        df.to_csv(args.output, index=False)
        print(f"\nData saved to: {args.output}")
        print(f"File size: {os.path.getsize(args.output) / 1024:.2f} KB")
        
        # Display sample data
        print(f"\nFirst 5 rows:")
        print(df.head())
        
        print(f"\nLast 5 rows:")
        print(df.tail())
        
        print(f"\nDownload completed successfully!")
    else:
        print("ERROR: No data downloaded")
        sys.exit(1)

if __name__ == "__main__":
    main()
