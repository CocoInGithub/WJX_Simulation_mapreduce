import os
import csv
import glob
from collections import defaultdict

# 1.Simulate files stored in HDFS by dividing the file into 4 splits
num_splits = 4
def split_file(filename, num_splits):
    with open(filename, 'r') as f:
        reader = csv.reader(f)
        data = list(reader)

    split_size = len(data) // num_splits
    for i in range(num_splits):
        split_data = data[i*split_size : (i+1)*split_size]
        with open(f'HDFS/split_{i}.csv', 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerows(split_data)


split_file('AComp_Passenger_data_no_error.csv', num_splits)


# 2.Map function: For each passenger id (K), return a number of flights (V).
def map_func(filename):
    intermediate = defaultdict(int)
    with open(filename, 'r') as f:
        reader = csv.reader(f)
        for row in reader:
            passenger_id = row[0]
            intermediate[passenger_id] += 1

    # Sort the intermediate results by key
    intermediate = sorted(intermediate.items(), key=lambda x: x[1], reverse=True)
    return intermediate

# 3.Buffer: Spill the splits to local disk
buffer_size = 12

# Create directory for temp files
os.makedirs("./tmp/mapred/local", exist_ok=True)

# Function to handle buffer, spill and sort etc
def buffer_and_spill(result_list, i):
    buffer = []
    buffer_count = 0
    for key_value_pair in result_list:
        # Add key-value pair to buffer
        buffer.append(key_value_pair)
        # Sort buffer based on values
        buffer.sort(key=lambda x: x[1])
        # Check if buffer is full
        if len(buffer) >= buffer_size:
            # Spill buffer to temp file
            filename = f"./tmp/mapred/local/split_{i}_buffer_{buffer_count}.csv"
            with open(filename, "w") as f:
                writer = csv.writer(f)
                writer.writerows(buffer)
            # Clear buffer
            buffer = []
            buffer_count += 1
    # Flush remaining key-value pairs in buffer
    if buffer:
        # Sort buffer based on values
        buffer.sort(key=lambda x: x[1])
        filename = f"./tmp/mapred/local/split_{i}_buffer_{buffer_count}.csv"
        with open(filename, "w") as f:
            writer = csv.writer(f)
            writer.writerows(buffer)

# Process each split file
intermediate_results = []
for i in range(4):
    filename = f'HDFS/split_{i}.csv'
    result = map_func(filename)
    intermediate_results.append(result)
    buffer_and_spill(result, i)

# Print intermediate results
# for i, result in enumerate(intermediate_results):
#    print(f'Split {i}: {result}')
#    print(len(result))


# 4.Final reduce
def reduce_func(intermediate_list):
    counts_dict = defaultdict(int)
    for item in intermediate_list:
        counts_dict[item[0]] += item[1]
    return counts_dict

# Get a list of all "split_i_buffer_j.csv" files
buffer_files = glob.glob('./tmp/mapred/local/split_*_buffer_*.csv')

# Read all intermediate files into a list
intermediate_data = []
for file in buffer_files:
    with open(file, 'r') as f:
        reader = csv.reader(f)
        for row in reader:
            # Check if the row has at least two elements
            if len(row) >= 2:
                # Convert the count back to int
                row[1] = int(row[1])
                intermediate_data.append(row)

# Sort the intermediate data
intermediate_data.sort(key=lambda x: x[1])

# Perform the Reduce operation
final_result = reduce_func(intermediate_data)

# Write the final result to a file
with open('result.csv', 'w') as f:
    writer = csv.writer(f)
    for key, value in final_result.items():
        writer.writerow([key, value])

# Find and print the passenger with the most flights
most_flights_passenger = max(final_result, key=final_result.get)
print(f'The id of passenger with the most flights is: {most_flights_passenger}')