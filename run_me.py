import subprocess
import argparse


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--rpc-url", type=str, help="RPC URL to use", default="https://fullnode.mainnet.sui.io:443")
    parser.add_argument("--input-filename", type=str, help="Input filename", default="input_addresses.csv")
    parser.add_argument("--end-epoch", type=int, help="End epoch", default=209)
    parser.add_argument("--output-filename", default="output.csv")

    args = parser.parse_args()

    script1 = 'v3.py'
    args1 = ['--input-filename', args.input_filename, '--rpc-url', args.rpc_url]
    command1 = ['python3', script1] + args1
    script2 = 'sui_tracker_v2.py'
    args2 = ['--end-epoch', str(args.end_epoch), '--input-filename', args.input_filename, '--rpc-url', args.rpc_url]
    command2 = ['python3', script2] + args2

    # Run the first script
    print(f"Running {script1}...")
    subprocess.run(command1, check=True)

    # After the first script finishes, run the second script
    print(f"Running {script2}...")
    subprocess.run(command2, check=True)

    print("Both scripts have finished executing.")


if __name__ == "__main__":
    main()
