import pandas as pd

dtypes_dict = {"topic": "string", "offset": "string", "key": "string",
                "value": "string", "timestamp": "string"}

default_split_str = "!!432&%$(())#"
iot_split_str = ","

def parse_input_data(input_data):
    input_topic = str(input_data.iloc[0]['topic'])
    values_list = input_data['value'].tolist()
    split_rows = lambda row: row.split(iot_split_str)[-1] if "iot" in input_topic else row.split(default_split_str)[-1]
    ids_list = []
    ids_list = map(split_rows, values_list)
    input_data['input_row_id'] = list(ids_list)
    parsed_input = input_data.rename(columns={'topic': 'input_topic', 'key': 'input_key',
                                               'value': 'input_value', 'timestamp': 'input_timestamp'})

    return parsed_input

def parse_output_data(output_data):
    ids = output_data['value'].str.findall(r'(?!!!432&%\$\(\(\)\)\#)\d+_\d+').tolist()
    topic_A_ids = list(map(lambda l: l[0].split("_")[0], ids))
    topic_B_ids = list(map(lambda l: l[0].split("_")[1], ids))
    output_data['id_topicA'], output_data['id_topicB'] = topic_A_ids, topic_B_ids
    parsed_output = output_data.rename(columns={'topic': 'output_topic','key': 'output_key',
                                                 'value': 'output_value', 'timestamp': 'output_timestamp'})
    return parsed_output

def calculate_avg_timestamp(tt1, tt2):
    return ((int(tt1) + int(tt2)) / 2)

def clean_and_process(parsed_input, parsed_output):
    avg_input_timestamps = []
    final_latencies      = []
    for index, row in parsed_output.iterrows():
        id_topicA, id_topicB = row['id_topicA'], row['id_topicB']
        rowA, rowB = parsed_input.loc[parsed_input['input_row_id'] == id_topicA], parsed_input.loc[parsed_input['input_row_id'] == id_topicB]
        
        avg_input_timestamp = calculate_avg_timestamp(rowA['input_timestamp'], rowB['output_timestamp'])
        avg_input_timestamps.append(avg_input_timestamp)

        final_latency = int(row['output_timestamp']) - avg_input_timestamp
        final_latencies.append(final_latency)

        print(parsed_input.loc[parsed_input['input_row_id'] == id_topicA])
        print(id_topicA)
        break
    parsed_output['avg_input_timestamp'], parsed_output['final_timestamp'] = avg_input_timestamps, final_latencies
    result = parsed_output.drop('offset', axis=1)
    return result

input_data = pd.read_csv("input_timestamps.csv", sep=";", dtype=dtypes_dict, skiprows=[1])
output_data = pd.read_csv("output_timestamps.csv", sep=";", dtype=dtypes_dict)
parsed_input = parse_input_data(input_data)
parsed_output = parse_output_data(output_data)
result = clean_and_process(parsed_input, parsed_output)
result.to_csv('timestamps_result', sep=';', encoding='utf-8')
print(parsed_input)
print(parsed_output)
print(result.loc[0])
print(result.columns)
