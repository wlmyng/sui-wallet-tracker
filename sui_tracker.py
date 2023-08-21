import requests
import json
import argparse
from datetime import datetime
from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any, Iterator
import csv
from timeout_decorator import timeout, timeout_decorator
from track_historical_staked_sui import get_all_sui_objs_at_epoch, calculate_rewards_for_address

class Stake(BaseModel):
    stakedSuiId: str
    stakeRequestEpoch: str
    stakeActiveEpoch: str
    principal: str
    status: str
    estimatedReward: Optional[str] = "0"

class Validator(BaseModel):
    validatorAddress: str
    stakingPool: str
    stakes: List[Stake]

class GetStakesResult(BaseModel):
    __root__: List[Validator]
    def __iter__(self) -> Iterator:
        return iter(self.__root__)

class GetBalanceResult(BaseModel):
    coinType: str
    coinObjectCount: int
    totalBalance: str
    lockedBalance: Dict[str, Any]

class StakeAndReward(BaseModel):
    total_principal: int
    total_estimated_reward: int

class CsvInput(BaseModel):
    address: str = Field(..., alias="Wallet Address")
    category: Optional[str] = Field(..., alias="Category")


def get_balance(url, owner, coin_type="0x2::sui::SUI"):
    """Get balance of liquid SUI from an address"""
    timestamp = datetime.now().strftime('%Y%m%d%H%M%S%f')
    payload = {
        "jsonrpc": "2.0",
        "id": timestamp,
        "method": "suix_getBalance",
        "params": [owner, coin_type]        
    }

    headers = {'content-type': 'application/json'}
    response = requests.post(url, data=json.dumps(payload), headers=headers).json()
    data = response['result']
    return GetBalanceResult(**data)

def get_stakes(url, owner):
    """Get stakes"""    
    timestamp = datetime.now().strftime('%Y%m%d%H%M%S%f')
    payload = {
        "jsonrpc": "2.0",
        "id": timestamp,
        "method": "suix_getStakes",
        "params": [owner]
    }

    headers = {'content-type': 'application/json'}
    response = requests.post(url, data=json.dumps(payload), headers=headers).json()
    data = response['result']    
    return GetStakesResult(__root__=[Validator(**v) for v in data])

def calculate_stake_and_reward(get_stakes_result: GetStakesResult) -> StakeAndReward:
    """
    Calculate total stake and estimated reward from get_stakes_result.
    An address may stake at multiple validators.
    For each validator, there may be multiple stakes
    """    
    total_stakes = 0
    total_principal = 0
    total_estimated_reward = 0
    for validator in get_stakes_result:
        for stake in validator.stakes:
            total_stakes += 1
            total_principal += int(stake.principal)
            total_estimated_reward += int(stake.estimatedReward)                
    return StakeAndReward(total_principal=total_principal, total_estimated_reward=total_estimated_reward)

def build_rows(address, name, liquid, staked, reward):
    name = "N/A" if not name else name
    rows = []
    for balance, type in zip([liquid, staked, reward], ["Liquid SUI", "Staked SUI", "Estimated Reward"]):
        rows.append([address, name, type, round((int(balance) / 1e9), 2)])
    return rows

def read_csv(filename: str) -> List[CsvInput]:
    with open(filename, 'r') as f:
        reader = csv.DictReader(f)
        return [CsvInput.parse_obj(row) for row in reader]        

@timeout(60)
def process_row(args, row: CsvInput, epoch: int = None):
    print(f"Processing {row.address}")
    if epoch:
        (staked_sui_objs, sui_coin_objs) = get_all_sui_objs_at_epoch(args.rpc_url, row.address, epoch)        
        print(staked_sui_objs)
        print(sui_coin_objs)
        (liquid_balance, total_principal, estimated_rewards) = calculate_rewards_for_address(args.rpc_url, epoch, staked_sui_objs, sui_coin_objs)        
        result = StakeAndReward(total_principal=total_principal, total_estimated_reward=estimated_rewards)
    else:
        liquid_balance = get_balance(args.rpc_url, row.address).totalBalance
        stakes = get_stakes(args.rpc_url, row.address)
        result = calculate_stake_and_reward(stakes)
    
    rows = build_rows(row.address, row.category, liquid_balance, result.total_principal, result.total_estimated_reward)
    return rows

def main():    
    parser = argparse.ArgumentParser()
    parser.add_argument("--rpc-url", type=str, help="RPC URL to use", default="https://fullnode.mainnet.sui.io:443")    
    parser.add_argument("--filename", default="test.csv")
    parser.add_argument("--epoch", type=int, help="Epoch to use", required=False)
    parser.add_argument("--append", action="store_true", help="Append to output.csv instead of overwriting it")
    parser.add_argument("--start-from", type=int, help="Start from a specific row in the CSV file", default=0)
    parser.add_argument("--cumulative", action="store_true", help="Calculate cumulative staked SUI", default=False)
    args = parser.parse_args()

    input_data = read_csv(args.filename)
    input_data = input_data[args.start_from:]

    mode = "a" if args.append else "w"    
    with open("output.csv", mode) as f:
        writer = csv.writer(f)
        if not args.append:
            writer.writerow(["Address", "Name", "Type", "Sui holdings"])  # Write the header row
    
        for row in input_data:
            epochs = [args.epoch] if not args.cumulative else list(range(0, args.epoch + 1))            
            for epoch in epochs:  
                print(epoch)              
                try:
                    rows = process_row(args, row, epoch)
                except timeout_decorator.TimeoutError:
                    print(f"Timeout processing {row.address}")
                    rows = build_rows(row.address, row.category, -1, -1, -1)
            
                # Write rows to the CSV file immediately after processing
                for r in rows:
                    writer.writerow(r)


if __name__ == "__main__":
    main()