import requests
import json
import argparse
from datetime import datetime
from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any, Iterator
import csv
from timeout_decorator import timeout, timeout_decorator
from track_historical_staked_sui import SuiClient, build_object_history_for_address, calculate_rewards_for_address
from sqlite_manager import SqliteManager

class CsvInput(BaseModel):
    address: str = Field(..., alias="Wallet Address")
    category: Optional[str] = Field(..., alias="Category")

def read_csv(filename: str) -> List[CsvInput]:
    with open(filename, 'r') as f:
        reader = csv.DictReader(f)
        return [CsvInput.parse_obj(row) for row in reader] 
    
def main():    
    parser = argparse.ArgumentParser()
    parser.add_argument("--rpc-url", type=str, help="RPC URL to use", default="https://fullnode.mainnet.sui.io:443")    
    parser.add_argument("--filename", default="test.csv")    
    parser.add_argument("--start-from", type=int, help="Start from a specific row in the CSV file", default=0)    
    args = parser.parse_args()

    input_data = read_csv(args.filename)
    input_data = input_data[args.start_from:]

    db = SqliteManager(version="v2")
    sui_client = SuiClient(url=args.rpc_url)

        
    for row in input_data:
        print(f"Processing {row.address}")
        try:        
            (staked_sui_objs, sui_coin_objs) = build_object_history_for_address(sui_client, row.address)
            db.insert_batch_staked_sui_v2(staked_sui_objs)
            db.insert_batch_sui_coin_v2(sui_coin_objs)
        except timeout_decorator.TimeoutError:
            print(f"Timeout processing {row.address}")
            continue
        print("Done")
        
            
if __name__ == "__main__":
    main()