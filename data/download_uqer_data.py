#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Generic script to download stock/index data from uqer (优矿)
Supports customizable stock code, K-line frequency, and date range
"""

import argparse
import pandas as pd
from datetime import datetime
import os
import sys

def download_index_minute_data(ticker, start_date, end_date, unit=1, token=None, adj_flag=None):
    """
    Download minute-level index data from uqer
    
    Parameters:
    -----------
    ticker : str
        Index code, e.g., '399300' for CSI 300
    start_date : str
        Start date in format 'YYYYMMDD'
    end_date : str
        End date in format 'YYYYMMDD'
    unit : int
        K-line unit (1, 5, 15, 30, 60 minutes)
    token : str
        uqer API token for authentication
    adj_flag : str, optional
        Adjustment flag: '1' for forward adjustment (前复权), '2' for backward adjustment (后复权), None for no adjustment
    
    Returns:
    --------
    pd.DataFrame
        Downloaded data
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
        
        print(f"\nDownloading data for {ticker}...")
        print(f"  Period: {start_date} to {end_date}")
        print(f"  Frequency: {unit}-minute")
        
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
        
        # Call API to get minute data
        # MktIdxdGet is for index minute data
        api_params = {
            'ticker': ticker_clean,
            'beginDate': start_date_formatted,
            'endDate': end_date_formatted,
            'unit': str(unit),
            'field': '',
            'pandas': '1'
        }
        
        # Add adjustment flag if specified
        if adj_flag:
            api_params['adjustFlag'] = str(adj_flag)
            adj_type = '前复权' if adj_flag == '1' else '后复权' if adj_flag == '2' else '不复权'
            print(f"  Adjustment: {adj_type}")
        
        df = DataAPI.MktIdxdGet(**api_params)
        
        if df is None or len(df) == 0:
            print("WARNING: No data returned from API")
            return None
        
        print(f"\nSuccessfully downloaded {len(df)} records")
        
        # Check available columns and print date range
        if 'barTime' in df.columns:
            print(f"Date range: {df['barTime'].min()} to {df['barTime'].max()}")
        elif 'tradeDate' in df.columns:
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

def download_stock_minute_data(ticker, start_date, end_date, unit=1, token=None, adj_flag=None):
    """
    Download minute-level stock data from uqer
    
    Parameters:
    -----------
    ticker : str
        Stock code, e.g., '000001.XSHE' for Ping An Bank
    start_date : str
        Start date in format 'YYYYMMDD'
    end_date : str
        End date in format 'YYYYMMDD'
    unit : int
        K-line unit (1, 5, 15, 30, 60 minutes)
    token : str
        uqer API token for authentication
    adj_flag : str, optional
        Adjustment flag: '1' for forward adjustment (前复权), '2' for backward adjustment (后复权), None for no adjustment
    
    Returns:
    --------
    pd.DataFrame
        Downloaded data
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
        
        print(f"\nDownloading data for {ticker}...")
        print(f"  Period: {start_date} to {end_date}")
        print(f"  Frequency: {unit}-minute")
        
        # Convert date format from YYYYMMDD to YYYY-MM-DD
        start_date_formatted = f"{start_date[:4]}-{start_date[4:6]}-{start_date[6:]}"
        end_date_formatted = f"{end_date[:4]}-{end_date[4:6]}-{end_date[6:]}"
        print(f"  Formatted dates: {start_date_formatted} to {end_date_formatted}")
        
        # Download data using MktEqudGet (equity minute data)
        api_params = {
            'ticker': ticker,
            'beginDate': start_date_formatted,
            'endDate': end_date_formatted,
            'unit': str(unit),
            'field': '',
            'pandas': '1'
        }
        
        # Add adjustment flag if specified
        if adj_flag:
            api_params['adjustFlag'] = str(adj_flag)
            adj_type = '前复权' if adj_flag == '1' else '后复权' if adj_flag == '2' else '不复权'
            print(f"  Adjustment: {adj_type}")
        
        df = DataAPI.MktEqudGet(**api_params)
        
        if df is None or len(df) == 0:
            print("WARNING: No data returned from API")
            return None
        
        print(f"\nSuccessfully downloaded {len(df)} records")
        
        # Check available columns and print date range
        if 'barTime' in df.columns:
            print(f"Date range: {df['barTime'].min()} to {df['barTime'].max()}")
        elif 'tradeDate' in df.columns:
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
    """Main function"""
    parser = argparse.ArgumentParser(
        description='Download stock/index data from uqer (优矿)',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Download CSI 300 index 1-minute data for 2025-2026
  python download_uqer_data.py -c 399300 -s 20250101 -e 20261231 -u 1 -t YOUR_TOKEN
  
  # Download stock data with 5-minute frequency
  python download_uqer_data.py -c 000001.XSHE -s 20250101 -e 20251231 -u 5 -t YOUR_TOKEN --type stock
  
  # Download 30-minute data
  python download_uqer_data.py -c 399300 -s 20250101 -e 20251231 -u 30 -t YOUR_TOKEN
        """
    )
    
    parser.add_argument('-c', '--code', required=True,
                       help='Stock/Index code (e.g., 399300 for CSI 300, 000001.XSHE for stock)')
    parser.add_argument('-s', '--start', required=True,
                       help='Start date in YYYYMMDD format (e.g., 20250101)')
    parser.add_argument('-e', '--end', required=True,
                       help='End date in YYYYMMDD format (e.g., 20261231)')
    parser.add_argument('-u', '--unit', type=int, default=1,
                       choices=[1, 5, 15, 30, 60],
                       help='K-line frequency in minutes (default: 1)')
    parser.add_argument('-t', '--token', required=True,
                       help='uqer API token (required for authentication)')
    parser.add_argument('--type', default='index', choices=['index', 'stock'],
                       help='Data type: index or stock (default: index)')
    parser.add_argument('--adj', choices=['1', '2'], 
                       help='Price adjustment: 1=forward (前复权), 2=backward (后复权), none=no adjustment')
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
    
    # Download data
    print("="*60)
    print("uqer Data Download Script")
    print("="*60)
    
    if args.type == 'index':
        df = download_index_minute_data(
            ticker=args.code,
            start_date=args.start,
            end_date=args.end,
            unit=args.unit,
            token=args.token,
            adj_flag=args.adj
        )
    else:
        df = download_stock_minute_data(
            ticker=args.code,
            start_date=args.start,
            end_date=args.end,
            unit=args.unit,
            token=args.token,
            adj_flag=args.adj
        )
    
    if df is None or len(df) == 0:
        print("\nNo data downloaded. Exiting.")
        sys.exit(1)
    
    # Generate output filename if not specified
    if args.output:
        output_file = args.output
    else:
        code_clean = args.code.replace('.', '_')
        output_file = f"{code_clean}_{args.start}_{args.end}_{args.unit}min.csv"
    
    # Save to CSV
    df.to_csv(output_file, index=False, encoding='utf-8')
    print(f"\nData saved to: {output_file}")
    print(f"Total records: {len(df)}")
    
    # Display sample data
    print("\nFirst 5 rows:")
    print(df.head())
    
    print("\n" + "="*60)
    print("Download Complete!")
    print("="*60)

if __name__ == "__main__":
    main()
