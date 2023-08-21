# Sui Tracker

## Setup

1. Install requirements with `pip3 install -r requirements.txt`
2. Run `python3 v3.py` with arguments to control which file and from where to start collecting historical objects from. This is done separately, as it's relatively easy to fetch the staked and liquid SUI objects from an address. This writes the data to a sqlite db called 'sui_data.db'.
3. Run `python3 sui_tracker_v2.py` to calculate estimated rewards for staked SUI.
4. Note that if you don't need the estimated rewards, you can use get_liquid_for_address_at_epoch or get_staked_for_address_at_epoch to get the liquid and staked SUI for an address at a given epoch. This is much faster than running the entire sui_tracker_v2.py script.

## Example usage

```python3
pip3 install -r requirements.txt
python3 v3.py --filename test.csv
python3 sui_tracker_v2.py --end-epoch 130 --filename test.csv
```

## Code walkthrough
v3.py file builds the historical object table:
1. Fetch all transactions where ToAddress and FromAddress are for the address of interest
2. Filter these transactions to only include those where StakedSui and Sui objects were created, mutated, or deleted
3. Take the object_id, version, status, and associate it with epoch information to create a dict of epoch -> `List[ObjectByEpoch]`
4. We try_multi_get_past_objects on this list, and then write to db. Each entry in the db is the object state at the epoch of creation/mutation/deletion.

Now that the bulk of the info has been gathered, we just need to calculate estimated rewards from staked sui.
1. Currently, the relationship of pool_id to validator_address is 1:1
2. So we make a query for EpochInfoV2 events, which emits per epoch for each validator
3. Now, for each staked sui object, we first try to find the validator_id/ address from the SuiSystemState
4. If this is not found, we do a convoluted object lookup to retrieve the validator_id
5. The estimated reward is calculated as `max(0, ((rate_at_activation_epoch) / rate_at_target_epoch) - 1.0) * principal)`. Note that if there is no information for rate_at_activation_epoch, we set this to 1. We similarly set rate_at_target_epoch to 1.