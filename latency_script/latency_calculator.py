import pandas as pd

dtypes_dict = {"topic": "string", "offset": "string", "key": "string",
                "value": "string", "timestamp": "string"}

default_split_str = "!!432&%$(())#"
iot_split_str = ","

def parse_input_data(input_data):
    global CURRENT_TOPIC
    input_topic = str(input_data.iloc[0]['topic'])
    CURRENT_TOPIC = input_topic[:-1]
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
        parsed_output['input_timestampA'], parsed_output['input_timestampB'] = pd.NA, pd.NA
        parsed_output['avg_input_timestamp'], parsed_output['final_latency'] = pd.NA, pd.NA
        total_idx = len(parsed_output)
        for index, row in parsed_output.iterrows():
            try:
                #if index % 10000 != 0:
                #    continue
                id_topicA, id_topicB = row['id_topicA'], row['id_topicB']
                rowA, rowB = parsed_input.loc[parsed_input['input_row_id'] == id_topicA], parsed_input.loc[parsed_input['input_row_id'] == id_topicB]
                
                avg_input_timestamp = calculate_avg_timestamp(rowA['input_timestamp'], rowB['input_timestamp'])

                final_latency = int(row['output_timestamp']) - avg_input_timestamp

                parsed_output.at[index, 'input_timestampA'], parsed_output.at[index, 'input_timestampB'] = rowA['input_timestamp'].values[0], rowB['input_timestamp'].values[0]
                parsed_output.at[index, 'avg_input_timestamp'], parsed_output.at[index, 'final_latency'] = avg_input_timestamp, final_latency
                print(f'index {index} de {total_idx} - {final_latency}')

            except Exception as e:
                print(rowA + " -- " +rowB)
                print("skipped")
                continue

        result = parsed_output.drop(['offset', 'output_key', 'output_topic'], axis=1)
        
        return result

input_data = pd.read_csv("input_timestamps.txt", sep=";", dtype=dtypes_dict) #si se cae con error de algo de series en timestamps eliminar skip row 
output_data = pd.read_csv("output_timestamps.txt", sep=";", dtype=dtypes_dict)
print("Files readed")
parsed_input = parse_input_data(input_data)
print("Parsed input data")
parsed_output = parse_output_data(output_data)
print("Parsed output data")
result = clean_and_process(parsed_input, parsed_output)
result = result[result['final_latency'].notna()]
result.to_csv(f'{CURRENT_TOPIC}_timestamps_result.csv', sep=';', encoding='utf-8')
print(result)
