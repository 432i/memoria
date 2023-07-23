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
    print(":D")

def merge_data(parsed_input, parsed_output):
    for index, row in parsed_output.iterrows():
        id_topicA, id_topicB = row['id_topicA'], row['id_topicB']
        rowA, rowB = parsed_input.loc[parsed_input['input_row_id'] == id_topicA], parsed_input.loc[parsed_input['input_row_id'] == id_topicB]
        
        print(parsed_input.loc[parsed_input['input_row_id'] == id_topicA])
        print(id_topicA)
        break
    #return merged_data

input_data = pd.read_csv("input_timestamps.csv", sep=";", dtype=dtypes_dict, skiprows=[1]);
output_data = pd.read_csv("output_timestamps.csv", sep=";", dtype=dtypes_dict);
parsed_input = parse_input_data(input_data)
parsed_output = parse_output_data(output_data)
merged_data = merge_data(parsed_input, parsed_output)
print(parsed_input)
print(parsed_output)
print(merged_data.loc[0])
print(merged_data.columns)
# ahora que los datos están mergeados hay que limpiar y sacar los promedios y latencia final
