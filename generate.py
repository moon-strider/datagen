import yaml
import asyncio
from langchain_openai import ChatOpenAI
from langchain.schema import SystemMessage, HumanMessage
import os
import csv


with open('config.yaml', 'r') as f:
    config = yaml.safe_load(f)

data_config = config.get('data-config', {})
generation_config = config.get('generation-config', {})
os.environ["OPENAI_API_KEY"] = config.get('openai-api-key')

if not os.environ.get("OPENAI_API_KEY"):
    raise KeyError("OPENAI_API_KEY not found in the config file or environment variables.")


chat = ChatOpenAI(model='gpt-4o-mini', temperature=0.5)


def create_system_prompt(data_config):
    instructions = "You are to generate CSV data with the following specifications:\n"
    instructions += "Each row should contain the following columns:\n"

    for field_name, field_spec in data_config.items():
        desc = field_spec.get('desc', '')
        field_type = field_spec.get('type', '')
        instructions += f"- {field_name}: {desc}. Type: {field_type}."
        if field_type.lower() in ['str', 'string']:
            min_length = field_spec.get('min_length')
            max_length = field_spec.get('max_length')
            if min_length is not None and max_length is not None:
                instructions += f" Minimum length: {min_length}. Maximum length: {max_length}."
        elif field_type.lower() in ['int', 'integer']:
            min_value = field_spec.get('min')
            max_value = field_spec.get('max')
            if min_value is not None and max_value is not None:
                instructions += f" Minimum value: {min_value}. Maximum value: {max_value}."
        instructions += "\n"

    instructions += "Ensure that the data satisfies the specified constraints.\n"
    instructions += "Do not include any extra text; only output valid CSV data."
    return instructions


system_prompt = create_system_prompt(data_config)


def create_human_message_template(datapoints):
    message = f"Please generate {datapoints} rows of data."
    return message


total_datapoints = generation_config.get('datapoints', 10)
batch_size = max(1, total_datapoints // 10)
num_batches = (total_datapoints + batch_size - 1) // batch_size


async def generate_data(chat, system_prompt, human_message):
    max_retries = 5
    delay = 1
    for _ in range(max_retries):
        try:
            messages = [
                SystemMessage(content=system_prompt),
                HumanMessage(content=human_message)
            ]
            response = await chat.apredict_messages(messages)
            return response.content
        except Exception as e:
            if '429' in str(e):
                print(f"Received 429 error, retrying after {delay} seconds...")
                await asyncio.sleep(delay)
                delay *= 2
            else:
                print(f"Error: {e}")
                break
    print("Failed to generate data after retries.")
    return None


async def main():
    tasks = []
    for i in range(num_batches):
        batch_datapoints = min(batch_size, total_datapoints - i * batch_size)
        human_message = create_human_message_template(batch_datapoints)
        task = generate_data(chat, system_prompt, human_message)
        tasks.append(task)

    results = await asyncio.gather(*tasks)

    data_rows = []
    header = None
    header_included = generation_config.get('header', True)

    for result in results:
        if result:
            reader = csv.reader(result.strip().split('\n'))
            rows = list(reader)
            if not rows:
                continue
            if header_included:
                if header is None:
                    header = rows[0]
                    data_rows.extend(rows[1:])
                else:
                    if rows[0] == header:
                        data_rows.extend(rows[1:])
                    else:
                        print("Headers do not match across batches.")
                        data_rows.extend(rows[1:])
            else:
                data_rows.extend(rows)

    unique_data = [list(x) for x in set(tuple(row) for row in data_rows)]

    output_dir = generation_config.get('output-dir', 'data')
    file_name = generation_config.get('file-name', 'data.csv')
    quantize = generation_config.get('quantize', 1)

    os.makedirs(output_dir, exist_ok=True)

    total_data_rows = len(unique_data)
    rows_per_file = (total_data_rows + quantize - 1) // quantize

    for i in range(quantize):
        start_idx = i * rows_per_file
        end_idx = min(start_idx + rows_per_file, total_data_rows)
        data_subset = unique_data[start_idx:end_idx]
        if header_included and header is not None:
            data_to_write = [header] + data_subset
        else:
            data_to_write = data_subset

        file_path = os.path.join(output_dir, f"{os.path.splitext(file_name)[0]}_{i+1}.csv")
        with open(file_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerows(data_to_write)

if __name__ == '__main__':
    asyncio.run(main())


# TODO: import fire
# TODO: load yaml config
# TODO: generation config (multiple files etc)
# TODO: structured outputs
# TODO: allow duplicated
# TODO: move temp, model, batchsize to config