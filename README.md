# Sui Tracker

## Using sui_tracker.py
Install requirements with `pip install -r requirements.txt`

Run with `python3 sui_tracker.py`, with optional args `--rpc-url` and `--filename`.

The script should be able to run on any csv input as long as it has `address` or `Wallet Address` and `category` or `Category` columns.

Produces an `output.csv` of `Address,Name,Type,Sui holdings`. Each input row will result in 3 rows, one for each of Liquid, Staked, and EstimatedReward.

## Using track_historical_staked_sui.py
`python3 track_historical_staked_sui.py --epoch 94 --address 0xAddress`
