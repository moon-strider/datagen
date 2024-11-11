# DataGen

This is a simple Python script that uses the OpenAI API to generate CSV data based on a configuration file.

## Installation

1. Clone the repository:

```bash
git clone https://github.com/moon-strider/DataGen.git
```

2. Install the required dependencies:

```bash
pip install -r requirements.txt
```

## Configuration

The configuration file is located at `config.yaml`. It contains the following sections:

- `data-config`: This section defines the data fields and their specifications.
- `generation-config`: This section defines the generation parameters, such as the number of datapoints to generate, the output directory, and the file name.

## API key

Create a file called secret.yaml in the root of the project and insert your OpenAI API key. Take the secret.example.yaml file as a template.