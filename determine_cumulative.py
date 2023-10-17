import csv

# Specify the CSV file path
csv_file_path = 'a.csv'

# Initialize a dictionary to hold the sum for each epoch
epoch_sums = {}

# Open the CSV file and create a CSV reader object

def to_float(s):
    try:
        return float(s)
    except ValueError:  # If conversion fails, return 0.0
        return 0.0
    
new_rows = []

with open(csv_file_path, 'r') as file:
    reader = csv.reader(file)
    header = next(reader) # skip header
    
    # Iterate over each row in the CSV
    for row in reader:
        # The epoch columns start from index 3 onwards
        if row[2] == 'Estimated Reward':
            new_rows.append(row[:2] + ['Estimated Reward for Epoch'] + row[3:])
            new_rows.append(row[:2] + ['Cumulative to Epoch'])
            estimated_rewards = row[3:]
            cumulative_rewards = [0 for i in range(len(estimated_rewards))]

            for i in range(len(estimated_rewards)):
                cumulative_rewards[i] = to_float(estimated_rewards[i]) + to_float(cumulative_rewards[max(0, i-1)])
            new_rows[-1].extend(cumulative_rewards)    
            
        # for i, value in enumerate(row[3:], start=3):
            # Convert the value to integer and add it to the corresponding epoch sum
            # epoch_sums[i] = epoch_sums.get(i, 0) + int(value)

with open('by_epoch_output.csv', 'w') as f:
    writer = csv.writer(f)
    writer.writerows(new_rows)
# Print the total sum for each epoch
# for epoch, total_sum in epoch_sums.items():
    # print(f'Total Sum for epoch {epoch - 3}: {total_sum}')
