import requests
import json
import argparse
from functools import lru_cache
from typing import List, Optional, Tuple, Dict, Union, Any
from pydantic import BaseModel, Field
from datetime import datetime
import os

@lru_cache(maxsize=128)
def query_transaction_blocks(filter_type, address, url='https://fullnode.mainnet.sui.io:443', cursor=None, limit=1000, descending_order=False):
    query = {
        "filter": {
            filter_type: address,            
        },
        "options": {
            "showInput": True,
            "showRawInput": False,
            "showEffects": True,
            "showEvents": True,
            "showObjectChanges": True,
            "showBalanceChanges": True
        }
    }

    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "suix_queryTransactionBlocks",
        "params": [query, cursor, limit, descending_order]
    }            
    headers = {'content-type': 'application/json'}
    response = requests.post(url, data=json.dumps(payload), headers=headers).json()    
    transactions = []

    while response:    
        data = response['result']['data']    
        transactions.extend(data)        
        cursor = response['result']['nextCursor']
        has_next_page = response['result']['hasNextPage']
        if has_next_page:
            params = [query, cursor, limit, descending_order]
            payload["params"] = params        
            response = requests.post(url, data=json.dumps(payload), headers=headers).json()
        else:
            break

    return transactions

class DeletedObject(BaseModel):
    digest: str
    object_id: str = Field(..., alias="objectId")
    version: int

class Effects(BaseModel):
    deleted: Optional[List[DeletedObject]]
    executed_epoch: str = Field(..., alias="executedEpoch")

class AddressOwner(BaseModel):
    address_owner: str = Field(..., alias="AddressOwner")

class ObjectChange(BaseModel):
    obj_digest: str = Field(..., alias="digest")
    object_id: str = Field(..., alias="objectId")
    object_type: str = Field(..., alias="objectType")
    type: str
    version: str
    owner: Union[AddressOwner, dict]

class Transaction(BaseModel):
    checkpoint: str
    tx_digest: str = Field(..., alias="digest")
    effects: Effects
    object_changes: List[Union[ObjectChange, dict]] = Field(..., alias="objectChanges")
    timestampMs: str

class ObjectByEpoch(BaseModel):
    digest: str
    object_id: str
    version: str
    status: str    

class OrganizedByObjectIdOptional(BaseModel):
    digest: str
    object_id: str
    version: int
    created: Optional[str] # epoch
    deleted: Optional[str] # epoch
    mutated: Optional[List[Tuple[str, str]]] = [] # epoch, version

class OrganizedByObjectId(BaseModel):
    digest: str
    object_id: str
    version: int
    created: int # epoch
    deleted: Optional[int] # epoch
    mutated: Optional[List[Tuple[int, int]]] = [] # epoch, version

class StakedSuiAtEpoch(BaseModel):
    object_id: str
    version: int

def get_existing_objects_at_epoch(objs_by_obj_id: Dict[str, OrganizedByObjectId], epoch) -> List[StakedSuiAtEpoch]:        
    existing_objects = []
    for object_id, obj in objs_by_obj_id.items():        
        if obj.created <= epoch and (obj.deleted is None or obj.deleted > epoch):
            version = obj.version            
            if obj.mutated:            
                for mutation_epoch, mutation_version in obj.mutated:                    
                    if mutation_epoch <= epoch:                                                
                        if mutation_version > version:                            
                            version = mutation_version                
            existing_objects.append(StakedSuiAtEpoch(object_id=object_id, version=version))                                        
    return existing_objects

def multi_get_objects(request: List[StakedSuiAtEpoch], url='https://fullnode.mainnet.sui.io:443'):
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "sui_multiGetObjects",
        "params": [[r.object_id for r in request],        
            {
                "showType": True,
                "showOwner": True,
                "showPreviousTransaction": True,
                "showDisplay": False,
                "showContent": True,
                "showBcs": False,
                "showStorageRebate": True
            }
        ]
    }
    headers = {'content-type': 'application/json'}
    response = requests.post(url, data=json.dumps(payload), headers=headers).json()
    return response

def chunked_requests(request: List[Any], chunk_size=50):
    """Yield successive chunks from the request."""
    for i in range(0, len(request), chunk_size):
        yield request[i:i + chunk_size]

def try_multi_get_past_objects(request: List[StakedSuiAtEpoch], url='https://fullnode.mainnet.sui.io:443'):
    final_result = []
    for chunk in chunked_requests(request):
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "sui_tryMultiGetPastObjects",
            "params": [
                [
                    {"objectId": r.object_id, "version": str(r.version)
                    } for r in chunk
                ],            
                {
                    "showType": True,
                    "showOwner": True,
                    "showPreviousTransaction": True,
                    "showDisplay": False,
                    "showContent": True,
                    "showBcs": False,
                    "showStorageRebate": True
                }
            ]
        }
        headers = {'content-type': 'application/json'}
        response = requests.post(url, data=json.dumps(payload), headers=headers).json()
        final_result.extend(response['result'])
    return final_result


@lru_cache(maxsize=128)
def get_sui_system_state(url='https://fullnode.mainnet.sui.io:443'):
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "suix_getLatestSuiSystemState",
        "params": []
    }
    headers = {'content-type': 'application/json'}
    response = requests.post(url, data=json.dumps(payload), headers=headers).json()
    return response['result']

@lru_cache(maxsize=128)
def get_dynamic_fields(parent_object_id, url='https://fullnode.mainnet.sui.io:443'):
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "suix_getDynamicFields",
        "params": [parent_object_id]
    }
    headers = {'content-type': 'application/json'}
    response = requests.post(url, data=json.dumps(payload), headers=headers).json()    
    return response['result']['data']

@lru_cache(maxsize=128)
def get_object(object_id, url='https://fullnode.mainnet.sui.io:443'):
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "sui_getObject",
        "params": [object_id, {
                    "showType": True,
                    "showOwner": True,
                    "showPreviousTransaction": True,
                    "showDisplay": False,
                    "showContent": True,
                    "showBcs": False,
                    "showStorageRebate": True
            }]        
    }
    headers = {'content-type': 'application/json'}
    response = requests.post(url, data=json.dumps(payload), headers=headers).json()
    return response['result']['data']

def find_closest(numbers, target):
    closest = None
    for num in numbers:
        if num <= target and (closest is None or abs(num - target) < abs(closest - target)):
            closest = num
    return closest

# made from misc.py -> query_validator_epoch_info_events
def query_validator_epoch_info_events(url):
    timestamp = datetime.now().strftime('%Y%m%d%H%M%S%f')
    events = []
    query = {
        "MoveEventType": "0x3::validator_set::ValidatorEpochInfoEventV2"
    }
    cursor = None
    limit = 1000

    payload = {
        "jsonrpc": "2.0",
        "id": timestamp,
        "method": "suix_queryEvents",
        "params": [query]        
    }

    headers = {'content-type': 'application/json'}
    response = requests.post(url, data=json.dumps(payload), headers=headers).json()
    while response:
        data = response['result']['data']        
        events.extend(data)
        cursor = response['result']['nextCursor']
        has_next_page = response['result']['hasNextPage']
        if has_next_page:
            params = [query, cursor, limit]
            payload['params'] = params
            response = requests.post(url, data=json.dumps(payload), headers=headers).json()
        else:
            break    
    return events

if not os.path.exists('events.json'):
    events = query_validator_epoch_info_events('https://fullnode.mainnet.sui.io:443')
    with open('events.json', 'w') as fout:
        json.dump(events, fout, indent=4, sort_keys=True)

with open('events.json', 'r') as fin:
    epoch_events = json.load(fin)

epoch_validator_event_dict = {(str(event['parsedJson']['epoch']), event['parsedJson']['validator_address']): event 
    for event in epoch_events}


def calculate_rewards(
        epoch_validator_event_dict,
        principal,
        pool_id='0x748a0ce980c3804d21267a4d359ac5c64bd40cb6a3e02a527b45f828cf8fd30d',
        activation_epoch=0,
        target_epoch=105
        ):        
    sui_system_state = get_sui_system_state()
    validator_id = None
    for validator in sui_system_state['activeValidators']:
        if validator['stakingPoolId'] == pool_id:
            validator_id = validator['suiAddress']            
            break
    if not validator_id:
        raise Exception(f"Could not find validator for pool {pool_id}")        
            
    rate_at_activation_epoch = 1
    rate_at_target_epoch = None
    
    for epoch, validator_address in [(activation_epoch, validator_id), (target_epoch, validator_id)]:
        event = epoch_validator_event_dict.get((str(epoch), validator_address))
        if event:            
            pool_token_amount = int(event['parsedJson']['pool_token_exchange_rate']['pool_token_amount'])
            sui_amount = int(event['parsedJson']['pool_token_exchange_rate']['sui_amount'])
            rate = pool_token_amount / sui_amount
            if epoch == activation_epoch:
                rate_at_activation_epoch = rate
            if epoch == target_epoch:        
                rate_at_target_epoch = rate

            if rate_at_activation_epoch and rate_at_target_epoch:
                break        
    estimated_reward = max(0, ((rate_at_activation_epoch / rate_at_target_epoch) - 1.0) * principal)    
    return rate_at_activation_epoch, rate_at_target_epoch, estimated_reward, validator_id

class RewardsForStakedSui(BaseModel):
    object_id: str = Field(..., alias="objectId")
    version: str
    stake_activation_epoch: int
    principal: float
    estimated_rewards: float
    rate_at_activation: float
    rate_at_target: float
    validator_id: str

def filter_transactions_for_staked_sui(address, transactions) -> List[Transaction]:
    transformed_transactions = [
        Transaction(**transaction) if not isinstance(transaction, Transaction) else transaction for transaction in transactions
    ]
    staked_sui = set()
    filtered_transactions = []
    for transaction in transformed_transactions:
        keep_object_changes = []
        for object_change in transaction.object_changes:
            if isinstance(object_change, ObjectChange):
                if isinstance(object_change.owner, AddressOwner):
                    if object_change.owner.address_owner == address and object_change.object_type == "0x3::staking_pool::StakedSui":
                        staked_sui.add(object_change.object_id)
                        keep_object_changes.append(object_change)
        keep_effects_deleted = []
        if transaction.effects.deleted is not None:
            for deleted in transaction.effects.deleted:
                if deleted.object_id in staked_sui:
                    keep_effects_deleted.append(deleted)
        
        if keep_object_changes or keep_effects_deleted:
            effects = transaction.effects.copy(update={
                "deleted": keep_effects_deleted
            })
            transaction = transaction.copy(update={
                "object_changes": keep_object_changes,
                "effects": effects
            })
            filtered_transactions.append(transaction)
    return filtered_transactions

def get_staked_sui_history(address, filtered_transactions: List[Transaction], record: bool = False) -> Dict[str, OrganizedByObjectId]:
    objs_by_epoch = {}
    for transaction in filtered_transactions:
        epoch = transaction.effects.executed_epoch
        if transaction.effects.executed_epoch not in objs_by_epoch:
            objs_by_epoch[epoch] = []
        objs = []
        if transaction.effects.deleted:
            objs = [ObjectByEpoch(
                digest = d.digest,
                object_id = d.object_id,
                version = d.version,
                status = "deleted") for d in transaction.effects.deleted]
        else:
            objs = [ObjectByEpoch(
                digest = o.obj_digest,
                object_id = o.object_id,
                version = o.version,
                status = o.type) for o in transaction.object_changes]
        objs_by_epoch[epoch].extend(objs)
    if record:
        with open(f"{address}_filtered.json", "w") as f:
            transactions_to_dict = [t.dict() for t in filtered_transactions]
            json.dump(transactions_to_dict, f, indent=4, sort_keys=True)
        with open(f"{address}_by_epoch.json", "w") as fout:
            objs_by_epoch_dict = {k: [o.dict() for o in v] for k, v in objs_by_epoch.items()}
            json.dump(objs_by_epoch_dict, fout, indent=4, sort_keys=True)
    
    objs_by_obj_id = {}
    for epoch, epoch_objs in objs_by_epoch.items():
        for epoch_obj in epoch_objs:
            if epoch_obj.object_id not in objs_by_obj_id:
                # objs_by_obj_id[epoch_obj.object_id] = OrganizedByObjectIdOptional(
                objs_by_obj_id[epoch_obj.object_id] = OrganizedByObjectIdOptional(
                    digest = epoch_obj.digest,
                    object_id = epoch_obj.object_id,
                    version = epoch_obj.version,
                    created = None,
                    deleted = None,
                )
            if epoch_obj.status == "created":
                objs_by_obj_id[epoch_obj.object_id] = objs_by_obj_id[epoch_obj.object_id].copy(update={
                    "created": epoch
                })
            elif epoch_obj.status == "mutated":
                mutated = objs_by_obj_id[epoch_obj.object_id].mutated
                # mutated = [(int(e), int(v)) for e, v in mutated]                
                # mutated.append((int(epoch), int(epoch_obj.version)))
                mutated.append((epoch, epoch_obj.version))
                objs_by_obj_id[epoch_obj.object_id] = objs_by_obj_id[epoch_obj.object_id].copy(update={
                    "mutated": mutated
                })
            elif epoch_obj.status == "deleted":
                objs_by_obj_id[epoch_obj.object_id] = objs_by_obj_id[epoch_obj.object_id].copy(update={
                    "deleted": epoch # int(epoch)
                })
            else:
                raise Exception(f"Unknown status {epoch_obj.status}")
    
    objs_dict = {k: v.dict() for k, v in objs_by_obj_id.items()}
    if record:
        with open(f"{address}_by_object_id.json", "w") as fout:
            json.dump(objs_dict, fout, indent=4, sort_keys=True)    
    
    objs_by_obj_id: Dict[str, OrganizedByObjectId] = {k: OrganizedByObjectId.parse_obj(v) for k, v in objs_dict.items()}

    return objs_by_obj_id

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--rpc-url", type=str, help="RPC URL to use", default="https://fullnode.mainnet.sui.io:443")    
    parser.add_argument("--filename", default="test.csv")
    parser.add_argument("--epoch", default=95, type=int)
    parser.add_argument("--record", default=False, action="store_true")
    parser.add_argument("--address", default=None, type=str)

    # just need total_principal, total_estimated_reward
    print("Load EpochInfoV2 events")
    if not os.path.exists('events.json'):
        events = query_validator_epoch_info_events('https://fullnode.mainnet.sui.io:443')
        with open('events.json', 'w') as fout:
            json.dump(events, fout, indent=4, sort_keys=True)

    with open('events.json', 'r') as fin:
        epoch_events = json.load(fin)
    epoch_validator_event_dict = {(str(event['parsedJson']['epoch']), event['parsedJson']['validator_address']): event 
    for event in epoch_events}

    args = parser.parse_args()
    address = args.address
    query_epoch = int(args.epoch)
    transactions = query_transaction_blocks("ToAddress", address)
    if args.record:
        with open(f"{address}_transactions.json", "w") as f:
            json.dump(transactions, f, indent=4, sort_keys=True)

    filtered_transactions = filter_transactions_for_staked_sui(address, transactions)
    objs_by_obj_id = get_staked_sui_history(address, filtered_transactions, args.record)
    existing_objects = get_existing_objects_at_epoch(objs_by_obj_id, query_epoch)
    past_objs = try_multi_get_past_objects(existing_objects)

    estimated_rewards = 0
    total_principal = 0
    results = []

    for past_obj in past_objs:        
        staked_sui_fields = past_obj['details']['content']['fields']
        stake_activation_epoch = int(staked_sui_fields['stake_activation_epoch'])
        principal = int(staked_sui_fields['principal'])    
        total_principal += principal
        pool_id = staked_sui_fields['pool_id']    
        result = calculate_rewards(epoch_validator_event_dict, principal, pool_id, stake_activation_epoch, query_epoch)
        results.append(result)    
        rewards_for_staked_sui = RewardsForStakedSui(**past_obj['details'], stake_activation_epoch=stake_activation_epoch, principal=principal / 1e9, estimated_rewards=result[2] / 1e9, rate_at_activation=result[0], rate_at_target=result[1], validator_id=result[3])
        print(rewards_for_staked_sui)
        estimated_rewards += result[2]
    print(total_principal / 1e9)
    print(estimated_rewards / 1e9)
    


if __name__ == "__main__":
    main()