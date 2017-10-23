# tap-slack

An unofficial [Singer](https://singer.io) tap for extracting data from the Slack
API.

Supported resources:

- Conversations (incremental):
  - IMs
  - MPIMs
  - Private channel messages
  - Public channel messages
- Emoji (:grin:) (full-table)
- Files (incremental)
- Team information (full-table)
- Users (full-table)
- User groups (full-table)

## Install

Clone this repository, and then install by running:

```bash

python setup.py install

```

## Run

#### Run the application

You'll need a legacy Slack API token, which can be obtained from [this page](https://api.slack.com/custom-integrations/legacy-tokens).

Run the application using:

```bash

tap-slack -c config.json -s state.json

```

where `config.json` contains the following:

```json
{
  "start_date": "2017-01-01T00:00:00Z",
  "token": "xoxp-deadbeef12345678..."
}
```

and `state.json` is a file (optional) containing only the value of the last state message.
