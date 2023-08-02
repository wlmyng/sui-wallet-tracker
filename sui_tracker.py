import requests
import json
import argparse
from datetime import datetime
from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any, Iterator
import csv

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
    category: str = Field(..., alias="Category")


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
            print(stake)
            total_stakes += 1
            total_principal += int(stake.principal)
            total_estimated_reward += int(stake.estimatedReward)            
    print(total_stakes)
    return StakeAndReward(total_principal=total_principal, total_estimated_reward=total_estimated_reward)

def build_rows(address, name, liquid, staked, reward):
    rows = []
    for balance, type in zip([liquid, staked, reward], ["Liquid SUI", "Staked SUI", "Estimated Reward"]):
        rows.append([address, name, type, round((int(balance) / 1e9), 2)])
    return rows

def read_csv(filename: str) -> List[CsvInput]:
    with open(filename, 'r') as f:
        reader = csv.DictReader(f)
        return [CsvInput.parse_obj(row) for row in reader]        

def process_row(args, row: CsvInput):
    print(f"Processing {row.address}")        
    liquid_balance = get_balance(args.rpc_url, row.address).totalBalance
    stakes = get_stakes(args.rpc_url, row.address)
    result = calculate_stake_and_reward(stakes)
    
    rows = build_rows(row.address, row.category, liquid_balance, result.total_principal, result.total_estimated_reward)
    return rows

def main():    
    parser = argparse.ArgumentParser()
    parser.add_argument("--rpc-url", type=str, help="RPC URL to use", default="https://fullnode.mainnet.sui.io:443")    
    parser.add_argument("--filename", default="test.csv")    
    args = parser.parse_args()

    input_data = read_csv(args.filename)
    output = []
    for row in input_data:
        rows = process_row(args, row)
        output.extend(rows)
    
    with open("output.csv", "w") as f:
        writer = csv.writer(f)
        writer.writerow(["Address", "Name", "Type", "Sui holdings"])        
        for row in output:
            writer.writerow(row)

if __name__ == "__main__":
    main()