# Sui Tracker

1. Install requirements with `pip install -r requirements.txt`
2. Run `python v3.py` with arguments to control which file and from where to start collecting historical objects from. This is done separately, as it's relatively easy to fetch the staked and liquid SUI objects from an address. This writes the data to a sqlite db called 'sui_data.db'.
3. Run `python sui_tracker_v2.py` to calculate estimated rewards for staked SUI.
4. Note that if you don't need the estimated rewards, you can use get_liquid_for_address_at_epoch or get_staked_for_address_at_epoch to get the liquid and staked SUI for an address at a given epoch. This is much faster than running the entire sui_tracker_v2.py script.