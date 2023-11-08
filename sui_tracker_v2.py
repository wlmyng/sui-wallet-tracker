import sqlite3
import argparse
import csv
import os
import json
from pydantic import BaseModel, Field
from typing import List, Optional
from track_historical_staked_sui import SuiClient, StakedSuiRef, SuiCoinRef, calculate_rewards_for_address

def get_liquid_for_address_at_epoch(address, query_epoch, db_path="sui_data.db") -> List[SuiCoinRef]:
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    query = """
    WITH LatestVersion AS (
        SELECT
            object_id,
            MAX(version) AS max_version
        FROM
            sui_coins_v2
        WHERE
            owner = ?
            AND at_epoch <= ?
        GROUP BY
            object_id
    )

    SELECT
        scv2.*
    FROM
        sui_coins_v2 scv2
    JOIN
        LatestVersion lv ON scv2.object_id = lv.object_id AND scv2.version = lv.max_version
    WHERE
        NOT scv2.deleted
    ORDER BY
        ABS(scv2.at_epoch - ?);
    """

    cursor.execute(query, (address, query_epoch, query_epoch))
    results = cursor.fetchall()

    cursor.close()
    conn.close()

    objects = []
    for row in results:
        objects.append(SuiCoinRef(
            object_id=row[0],
            version=row[1],
            at_epoch=row[2],
            owner=row[3],
            balance=row[4],
            deleted=row[5]))

    return objects

def get_staked_for_address_at_epoch(address, query_epoch, db_path="sui_data.db") -> List[StakedSuiRef]:
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    query = """
    WITH LatestVersion AS (
        SELECT
            object_id,
            MAX(version) AS max_version
        FROM
            staked_sui_v2
        WHERE
            owner = ?
            AND at_epoch <= ?
        GROUP BY
            object_id
    )

    SELECT
        ssv2.*
    FROM
        staked_sui_v2 ssv2
    JOIN
        LatestVersion lv ON ssv2.object_id = lv.object_id AND ssv2.version = lv.max_version
    WHERE
        NOT ssv2.deleted
    ORDER BY
        ABS(ssv2.at_epoch - ?);
    """

    cursor.execute(query, (address, query_epoch, query_epoch))
    results = cursor.fetchall()

    cursor.close()
    conn.close()

    objects = []
    for row in results:
        objects.append(StakedSuiRef(
            object_id=row[0],
            version=row[1],
            at_epoch=row[2],
            owner=row[3],
            pool_id=row[4],
            principal=row[5],
            stake_activation_epoch=row[6],
            deleted=row[7]))

    return objects

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
    parser.add_argument("--start-epoch", type=int, help="Epoch to start at", default=0)
    parser.add_argument("--end-epoch", type=int, help="Epoch to end at", default=130)
    parser.add_argument("--append", action="store_true", help="Append to output.csv instead of overwriting it")
    parser.add_argument("--start-from", type=int, help="Start from a specific row in the CSV file", default=0)
    parser.add_argument("--liquid-sui", action="store_true", help="Calculate liquid SUI", default=True)
    parser.add_argument("--staked-sui", action="store_true", help="Calculate staked SUI", default=True)
    parser.add_argument("--estimated-rewards", action="store_true", help="Calculate estimated rewards", default=False)
    parser.add_argument("--use-previous-epoch", action="store_true", help="Use previous epoch for estimated rewards", default=False)

    args = parser.parse_args()

    if args.estimated_rewards and not args.staked_sui:
        raise Exception("Cannot calculate estimated rewards without staked SUI")

    sui_client = SuiClient(args.rpc_url)

    input_data = read_csv(args.filename)
    input_data = input_data[args.start_from:]

    if not os.path.exists('events.json'):
        print("Need to make initial fetch for EpochInfoV2 events")
        epoch_events = sui_client.query_validator_epoch_info_events()
        with open('events.json', 'w') as f:
            json.dump(epoch_events, f, indent=4, sort_keys=True)
    else:
        with open('events.json', 'r') as f:
            epoch_events = json.load(f)
        if int(epoch_events[-1]['parsedJson']['epoch']) < int(args.end_epoch):
            print("Need to make additional fetch for EpochInfoV2 events")
            new_epoch_events = sui_client.query_validator_epoch_info_events(epoch_events[-1]['id'])
            epoch_events.extend(new_epoch_events)
            with open('events.json', 'w') as f:
                json.dump(epoch_events, f, indent=4, sort_keys=True)
    epoch_validator_event_dict = {(str(event['parsedJson']['epoch']), event['parsedJson']['validator_address']): event
    for event in epoch_events}


    mode = "a" if args.append else "w"
    epochs = list(range(args.start_epoch, args.end_epoch + 1))
    with open("a.csv", mode) as f:
        writer = csv.writer(f)
        if not args.append:
            header = ["Address", "Name", "Type"]
            header.extend(epochs)
            writer.writerow(header)

        # iterate through each address
        for row in input_data:
            print(f"Processing {row.address}")
            data_to_write = {}
            for epoch in epochs:
                sui_coin_objs = get_liquid_for_address_at_epoch(row.address, epoch)
                liquid_balance = 0
                for sui_coin_obj in sui_coin_objs:
                    liquid_balance += sui_coin_obj.balance
                staked_sui_objs = get_staked_for_address_at_epoch(row.address, epoch)
                # calculate the cumulative rewards earned up to the 'epoch'
                stake_results = calculate_rewards_for_address(sui_client, epoch_validator_event_dict, args.start_epoch, epoch, staked_sui_objs, args.use_previous_epoch)
                if args.use_previous_epoch:
                    estimated_rewards = stake_results[1] / 1e9
                else:
                    estimated_rewards = round( (int(stake_results[1]) / 1e9), 2)
                data_to_write[epoch] = (
                    round( (int(liquid_balance) / 1e9), 2),
                    round( (int(stake_results[0]) / 1e9), 2),
                    estimated_rewards
                )

            name = row.category if row.category else ""
            prefix = [row.address, name]
            for i, type in enumerate(["Liquid SUI", "Staked SUI", "Estimated Reward"]):
                writer.writerow(prefix + [type] + [item[i] for item in data_to_write.values()])


if __name__ == "__main__":
    main()
