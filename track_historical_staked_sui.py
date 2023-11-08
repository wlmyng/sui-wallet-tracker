import requests
import json
import argparse
from functools import lru_cache
from typing import List, Optional, Tuple, Dict, Union, Any
from pydantic import BaseModel, Field
from datetime import datetime
import os

import requests
import json
from typing import List
from functools import lru_cache
from timeout_decorator import timeout

class SuiClient:
    def __init__(self, url='https://fullnode.mainnet.sui.io:443'):
        self.url = url
        self.headers = {'content-type': 'application/json'}

    def query_validator_epoch_info_events(self, cursor=None):
        timestamp = datetime.now().strftime('%Y%m%d%H%M%S%f')
        events = []
        query = {
            "MoveEventType": "0x3::validator_set::ValidatorEpochInfoEventV2"
        }
        limit = 1000

        payload = {
            "jsonrpc": "2.0",
            "id": timestamp,
            "method": "suix_queryEvents",
            "params": [query, cursor, limit, False]
        }

        headers = {'content-type': 'application/json'}
        response = requests.post(self.url, data=json.dumps(payload), headers=headers).json()
        while response:
            data = response['result']['data']
            events.extend(data)
            cursor = response['result']['nextCursor']
            has_next_page = response['result']['hasNextPage']
            if has_next_page:
                params = [query, cursor, limit]
                payload['params'] = params
                response = requests.post(self.url, data=json.dumps(payload), headers=headers).json()
            else:
                break
        return events

    @lru_cache(maxsize=128)
    def get_sui_system_state(self):
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "suix_getLatestSuiSystemState",
            "params": []
        }
        response = requests.post(self.url, data=json.dumps(payload), headers=self.headers).json()
        return response['result']

    @lru_cache(maxsize=128)
    def get_dynamic_fields(self, parent_object_id):
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "suix_getDynamicFields",
            "params": [parent_object_id]
        }
        response = requests.post(self.url, data=json.dumps(payload), headers=self.headers).json()
        return response['result']['data']

    @lru_cache(maxsize=128)
    def get_object(self, object_id):
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
        response = requests.post(self.url, data=json.dumps(payload), headers=self.headers).json()
        return response['result']['data']

    def multi_get_objects(self, request: List):
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
        response = requests.post(self.url, data=json.dumps(payload), headers=self.headers).json()
        return response

    def try_multi_get_past_objects(self, request: List):
        final_result = []
        for chunk in self.chunked_requests(request):
            payload = {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "sui_tryMultiGetPastObjects",
                "params": [
                    [{"objectId": r.object_id, "version": str(r.version)} for r in chunk],
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
            response = requests.post(self.url, data=json.dumps(payload), headers=self.headers).json()
            final_result.extend(response['result'])
        return final_result

    @lru_cache(maxsize=128)
    def query_transaction_blocks(self, filter_type, address, cursor=None, limit=1000, descending_order=False):
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
        transactions = []

        while True:
            response = requests.post(self.url, data=json.dumps(payload), headers=self.headers).json()
            data = response['result']['data']
            transactions.extend(data)
            cursor = response['result']['nextCursor']
            has_next_page = response['result']['hasNextPage']
            if not has_next_page:
                break
            payload["params"] = [query, cursor, limit, descending_order]

        return transactions

    def chunked_requests(self, request: List, chunk_size=50):
        for i in range(0, len(request), chunk_size):
            yield request[i:i + chunk_size]


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
    checkpoint: str = None
    tx_digest: str = Field(..., alias="digest")
    effects: Effects
    object_changes: List[Union[ObjectChange, dict]] = Field(..., alias="objectChanges")
    timestampMs: str = None

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
    created: Optional[int]
    deleted: Optional[int] # epoch
    mutated: Optional[List[Tuple[int, int]]] = [] # epoch, version

class ObjectAtEpoch(BaseModel):
    object_id: str
    version: int

class RewardsForStakedSui(BaseModel):
    object_id: str = Field(..., alias="objectId")
    version: str
    stake_activation_epoch: int
    principal: float
    estimated_rewards: float
    rate_at_activation: float
    rate_at_target: float
    validator_id: str
    pool_id: str

class StakedSuiRef(BaseModel):
    object_id: str
    version: int
    owner: str
    pool_id: str
    principal: int
    stake_activation_epoch: int
    at_epoch: int
    deleted: bool

class SuiCoinRef(BaseModel):
    object_id: str
    version: int
    owner: str
    balance: int
    at_epoch: int
    deleted: bool

class DeletedObjectRef(BaseModel):
    object_id: str
    version: int
    at_epoch: int
    owner: str
    deleted: bool

def get_existing_objects_at_epoch(objs_by_obj_id: Dict[str, OrganizedByObjectId], epoch) -> List[ObjectAtEpoch]:
    existing_objects = []
    for object_id, obj in objs_by_obj_id.items():

        # expect obj to have at least one of created or mutated
        if obj.created is None and obj.mutated is None:
            continue
        # filter out objects that were deleted before or in the epoch
        if obj.deleted is not None and obj.deleted <= epoch:
            continue

        version = None
        if obj.created is not None and obj.created <= epoch:
            version = obj.version
        # In the case of a transferred object, we may not have information on when the object was created
        if obj.mutated is not None:
            for mutation_epoch, mutation_version in obj.mutated:
                if mutation_epoch <= epoch:
                    if version is None or mutation_version > version:
                        version = mutation_version
        if version is not None:
            existing_objects.append(ObjectAtEpoch(object_id=object_id, version=version))
    return existing_objects

def chunked_requests(request: List[Any], chunk_size=50):
    """Yield successive chunks from the request."""
    for i in range(0, len(request), chunk_size):
        yield request[i:i + chunk_size]

def find_closest(numbers, target):
    closest = None
    for num in numbers:
        if num <= target and (closest is None or abs(num - target) < abs(closest - target)):
            closest = num
    return closest

def get_validator_id_for_inactive_pool(sui_client: SuiClient, pool_id, sui_system_state):
    inactive_pools = sui_client.get_dynamic_fields(sui_system_state['inactivePoolsId'])
    print(inactive_pools)
    validator_wrapper = None
    for item in inactive_pools:
        if item['name']['value'] == pool_id:
            validator_wrapper = item['objectId']
            break

    if validator_wrapper is None:
        raise Exception(f"Could not find validator wrapper for pool {pool_id}")

    validator_wrapper_object = sui_client.get_object(validator_wrapper)
    validator_dynamic_fields_id = validator_wrapper_object['content']['fields']['value']['fields']['inner']['fields']['id']['id']

    validator_object = sui_client.get_dynamic_fields(validator_dynamic_fields_id)
    validator_object_id = validator_object[0]['objectId']

    validator_object = sui_client.get_object(validator_object_id)
    validator_address = validator_object['content']['fields']['value']['fields']['metadata']['fields']['sui_address']
    return validator_address

def calculate_rewards(
        sui_client,
        sui_system_state,
        epoch_validator_event_dict,
        principal,
        pool_id,
        activation_epoch,
        target_epoch=105,
        ):

    validator_id = None
    for validator in sui_system_state['activeValidators']:
        if validator['stakingPoolId'] == pool_id:
            validator_id = validator['suiAddress']
            break
    if not validator_id:
        validator_id = get_validator_id_for_inactive_pool(sui_client, pool_id, sui_system_state)
        # raise Exception(f"Could not find validator for pool {pool_id} at activation epoch {activation_epoch}, target epoch {target_epoch}")

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

    rate_at_target_epoch = 1 if rate_at_target_epoch is None else rate_at_target_epoch
    estimated_reward = max(0, ((rate_at_activation_epoch / rate_at_target_epoch) - 1.0) * principal)

    print(activation_epoch, target_epoch, rate_at_activation_epoch, rate_at_target_epoch, principal, estimated_reward, validator_id)

    return rate_at_activation_epoch, rate_at_target_epoch, estimated_reward, validator_id

def filter_transactions_for_object_type(address, transactions, object_type="0x3::staking_pool::StakedSui") -> List[Transaction]:
    transformed_transactions = [
        Transaction(**transaction) if not isinstance(transaction, Transaction) else transaction for transaction in transactions
    ]
    object_id_set = set()
    filtered_transactions = []
    for transaction in transformed_transactions:
        keep_object_changes = []
        for object_change in transaction.object_changes:
            if isinstance(object_change, ObjectChange):
                if isinstance(object_change.owner, AddressOwner):
                    if object_change.owner.address_owner == address and object_change.object_type == object_type:
                        object_id_set.add(object_change.object_id)
                        keep_object_changes.append(object_change)
        keep_effects_deleted = []
        if transaction.effects.deleted is not None:
            for deleted in transaction.effects.deleted:
                if deleted.object_id in object_id_set:
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

def build_object_history(address, filtered_transactions: List[Transaction], record: bool = False) -> Tuple[Dict[str, List[ObjectByEpoch]], Dict[str, OrganizedByObjectId]]:
    objs_by_epoch: Dict[str, List[ObjectByEpoch]] = {}
    for transaction in filtered_transactions:
        epoch = transaction.effects.executed_epoch
        if transaction.effects.executed_epoch not in objs_by_epoch:
            objs_by_epoch[epoch] = []
        objs = []
        if transaction.effects.deleted:
            objs.extend([ObjectByEpoch(
                digest = d.digest,
                object_id = d.object_id,
                version = d.version,
                status = "deleted") for d in transaction.effects.deleted])
        if transaction.object_changes:
            objs.extend([ObjectByEpoch(
                digest = o.obj_digest,
                object_id = o.object_id,
                version = o.version,
                status = o.type) for o in transaction.object_changes])

        # validation
        unique_object_ids = set()
        for obj in objs:
            if obj.object_id in unique_object_ids:
                raise Exception(f"Duplicate object id {obj.object_id} in transaction {transaction}")
            unique_object_ids.add(obj.object_id)
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

    return (objs_by_epoch, objs_by_obj_id)

@timeout(60)
def get_all_sui_objs_at_epoch(sui_client: SuiClient, address, epoch, record=False) -> Tuple[List[StakedSuiRef], List[SuiCoinRef]]:
    print("Load EpochInfoV2 events")
    query_epoch = int(epoch)
    transactions = sui_client.query_transaction_blocks("ToAddress", address)
    if record:
        with open(f"{address}_transactions.json", "w") as f:
            json.dump(transactions, f, indent=4, sort_keys=True)
    filtered_transactions = filter_transactions_for_object_type(address, transactions)
    objs_by_epoch, objs_by_obj_id = build_object_history(address, filtered_transactions, record)
    existing_objects = get_existing_objects_at_epoch(objs_by_obj_id, query_epoch)
    past_objs = sui_client.try_multi_get_past_objects(existing_objects)

    staked_sui_objs = []
    for past_obj in past_objs:
        staked_sui_fields = past_obj['details']['content']['fields']
        stake_activation_epoch = int(staked_sui_fields['stake_activation_epoch'])
        principal = int(staked_sui_fields['principal'])
        pool_id = staked_sui_fields['pool_id']
        staked_sui_ref = StakedSuiRef(
            object_id=past_obj['details']['objectId'],
            version=past_obj['details']['version'],
            type=past_obj['details']['type'],
            owner=past_obj['details']['owner']['AddressOwner'],
            pool_id=pool_id,
            principal=principal,
            stake_activation_epoch=stake_activation_epoch,
            at_epoch=epoch
        )
        staked_sui_objs.append(staked_sui_ref)

    transactions.extend(sui_client.query_transaction_blocks("FromAddress", address))
    if record:
        with open(f"{address}_transactions.json", "w") as f:
            json.dump(transactions, f, indent=4, sort_keys=True)
    filtered_transactions = filter_transactions_for_object_type(address, transactions, "0x2::coin::Coin<0x2::sui::SUI>")
    objs_by_epoch, objs_by_obj_id = build_object_history(address, filtered_transactions, record)
    existing_objects = get_existing_objects_at_epoch(objs_by_obj_id, query_epoch)
    past_objs = sui_client.try_multi_get_past_objects(existing_objects)

    sui_coin_objs = []
    for past_obj in past_objs:
        sui_coin_objs.append(SuiCoinRef(
            object_id=past_obj['details']['objectId'],
            version=past_obj['details']['version'],
            type=past_obj['details']['type'],
            owner=past_obj['details']['owner']['AddressOwner'],
            balance=int(past_obj['details']['content']['fields']['balance']),
            at_epoch=epoch
        ))

    return (staked_sui_objs, sui_coin_objs)

@timeout(60)
def build_object_history_for_address(sui_client: SuiClient, address, record=False) -> Tuple[List[Union[StakedSuiRef, DeletedObjectRef]], List[Union[SuiCoinRef, DeletedObjectRef]]]:
    print("Load EpochInfoV2 events")
    transactions = sui_client.query_transaction_blocks("ToAddress", address)
    if record:
        with open(f"{address}_transactions.json", "w") as f:
            json.dump(transactions, f, indent=4, sort_keys=True)
    filtered_transactions = filter_transactions_for_object_type(address, transactions)
    objs_by_epoch, objs_by_obj_id = build_object_history(address, filtered_transactions, record)
    flattened = [(key, obj) for key, obj_list in objs_by_epoch.items() for obj in obj_list]

    at_epochs = [item[0] for item in flattened]
    past_objs = sui_client.try_multi_get_past_objects([item[1] for item in flattened])

    staked_sui_objs = []
    for idx, past_obj in enumerate(past_objs):
        try:
            staked_sui_fields = past_obj['details']['content']['fields']
            stake_activation_epoch = int(staked_sui_fields['stake_activation_epoch'])
            principal = int(staked_sui_fields['principal'])
            pool_id = staked_sui_fields['pool_id']
            staked_sui_ref = StakedSuiRef(
                object_id=past_obj['details']['objectId'],
                version=past_obj['details']['version'],
                type=past_obj['details']['type'],
                owner=past_obj['details']['owner']['AddressOwner'],
                pool_id=pool_id,
                principal=principal,
                stake_activation_epoch=stake_activation_epoch,
                at_epoch=at_epochs[idx],
                deleted=False
            )
        except:
            staked_sui_ref = DeletedObjectRef(
                object_id=flattened[idx][1].object_id,
                version=flattened[idx][1].version,
                at_epoch=at_epochs[idx],
                owner=address,
                deleted=True
            )
        staked_sui_objs.append(staked_sui_ref)

    transactions.extend(sui_client.query_transaction_blocks("FromAddress", address))
    if record:
        with open(f"{address}_transactions.json", "w") as f:
            json.dump(transactions, f, indent=4, sort_keys=True)
    filtered_transactions = filter_transactions_for_object_type(address, transactions, "0x2::coin::Coin<0x2::sui::SUI>")
    objs_by_epoch, objs_by_obj_id = build_object_history(address, filtered_transactions, record)
    flattened = [(key, obj) for key, obj_list in objs_by_epoch.items() for obj in obj_list]

    at_epochs = [item[0] for item in flattened]
    past_objs = sui_client.try_multi_get_past_objects([item[1] for item in flattened])

    sui_coin_objs = []
    for idx, past_obj in enumerate(past_objs):
        try:
            sui_coin_objs.append(SuiCoinRef(
                object_id=past_obj['details']['objectId'],
                version=past_obj['details']['version'],
                type=past_obj['details']['type'],
                owner=past_obj['details']['owner']['AddressOwner'],
                balance=int(past_obj['details']['content']['fields']['balance']),
                at_epoch=at_epochs[idx],
                deleted=False
            ))
        except:
            sui_coin_objs.append(DeletedObjectRef(
                object_id=flattened[idx][1].object_id,
                version=flattened[idx][1].version,
                at_epoch=at_epochs[idx],
                owner=address,
                deleted=True
            ))

    return (staked_sui_objs, sui_coin_objs)

def calculate_rewards_for_address(sui_client: SuiClient, epoch_validator_event_dict, start_epoch, end_epoch, staked_sui_objs: List[StakedSuiRef], use_previous_epoch=False) -> Tuple[int, int]:
    staked_sui = 0
    estimated_rewards = 0
    sui_system_state = sui_client.get_sui_system_state()

    gather = []
    for staked_sui_obj in staked_sui_objs:
        if use_previous_epoch:
            activation_epoch = max(staked_sui_obj.stake_activation_epoch, end_epoch - 1, 0)
        else:
            activation_epoch = max(staked_sui_obj.stake_activation_epoch, start_epoch)
        result = calculate_rewards(sui_client, sui_system_state, epoch_validator_event_dict,
                                   staked_sui_obj.principal,
                                   staked_sui_obj.pool_id,
                                   activation_epoch,
                                   end_epoch)
        gather.append(RewardsForStakedSui(
            objectId=staked_sui_obj.object_id,
            version=staked_sui_obj.version,
            stake_activation_epoch=staked_sui_obj.stake_activation_epoch,
            principal=staked_sui_obj.principal,
            estimated_rewards=result[2],
            rate_at_activation=result[0],
            rate_at_target=result[1],
            validator_id=result[3],
            pool_id=staked_sui_obj.pool_id,
        ))
        print(result[2], "outside")
        estimated_rewards += result[2]
        print(estimated_rewards)
        staked_sui += staked_sui_obj.principal

    return (staked_sui, estimated_rewards)
