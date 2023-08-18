# Sui Tracker

## Using sui_tracker.py
Install requirements with `pip install -r requirements.txt`

Run with `python3 sui_tracker.py`, with the following optional arguments:
--rpc-url: defaults to https://fullnode.mainnet.sui.io:443
--filename: to the csv file to read from: defaults to `input.csv`
--epoch: which epoch to gather results for
--append: during this process, we may encounter issues that cause the program to stop. Setting this flag will append to the output file instead of overwriting it.
--start-from: similar to the above, we may want to start from a specific location in the input file instead of the beginning.

The script should be able to run on any csv input as long as it has `address` or `Wallet Address` and `category` or `Category` columns.

Produces an `output.csv` of `Address,Name,Type,Sui holdings`. Each input row will result in 3 rows, one for each of Liquid, Staked, and EstimatedReward.

## Using track_historical_staked_sui.py
`python3 track_historical_staked_sui.py --epoch 94 --address 0xAddress`
