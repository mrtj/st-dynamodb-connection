# Streamlit DynamoDB Connection

Streamlit DynamoDB Connection is a Python library that connects your Streamlit application to an Amazon DynamoDB table. This integration allows you to read and write persistent data in your table as simple as it were a Python dictionary. You can also visualize and edit the data from the user interface of your the application using an intuitive table widget.

[Streamlit](https://streamlit.io) is an open-source Python library that allows developers to create web applications for machine learning and data science projects quickly and easily.

[Amazon DynamoDB](https://aws.amazon.com/dynamodb/) is a fully managed NoSQL database service provided by [Amazon Web Services (AWS)](http://aws.amazon.com).

The primary method of data access in DynamoDB involves retrieving schema-less items by specifying their key, a process quite similar to the Python dictionary API. This library builds on the [dynamodb-mapping](https://github.com/mrtj/dynamodb-mapping) Python package that implements the dictionary interface over a DynamoDB table.

## Demo application

You can try the demo application of DynamoDB Connection at [https://st-dynamodb-connection.streamlit.app](https://st-dynamodb-connection.streamlit.app).

![Demo application](https://github.com/mrtj/st-dynamodb-connection/blob/main/docs/pandas_api.png?raw=true "Demo application")

## Getting started

### Installation

You can install DynamoDB Connections with pip:

```shell
pip install st-dynamodb-connection
```

### Creating a DynamoDB table and configuring AWS credentials

1. As Streamlit DynamoDB Connections connects your Streamlit application to a DynamoDB table, first you need a DynamoDB table. If you do not have already one, you can follow the steps in the [Create a DynamoDB table and add some data](./docs/create_table.md) document to create one.

2. You will also need to configure the AWS credentials for your Streamlit app. Follow the steps described in the [Create Access Key](https://docs.streamlit.io/knowledge-base/tutorials/databases/aws-s3#create-access-keys) section of the Connect Streamlit to AWS S3 page of the Streamlit documentation. You can also add the AWS credentials to the connection-specific section of your `secrets.toml`. For example, if you name your connection `my_dynamodb_connection`, you can add the following in the secrets file:

    ```conf
    # .streamlit/secrets.toml

    [connections.my_dynamodb_connection]
    table_name = "my_table"
    aws_access_key_id = "xxx"
    aws_secret_access_key = "xxx"
    aws_region = "eu-west-1"
    ```

    Ensure you change the values to the actual value of your credentials, AWS region and table name.

### Using DynamoDB Connection

You can use DynamoDB Connection in your Streamlit application as simple as:

```python
import streamlit as st
from dynamodb_connection import DynamoDBConnection

# Create a connection:
conn = st.connection(
    "my_dynamodb_connection", type=DynamoDBConnection, api_type="pandas"
)

# Get all items in the table:
st.write(conn.items())

# Get a single item by key:
item = conn.get_item("first_item")
st.write(item)

# Put an item in the table:
conn.put_item(
    "new_item",
    {
        "text": "This item was put from streamlit!",
        "metadata": {"source": "mrtj"},
    }
)

# Modify an existing item:
conn.modify_item(
    "new_item",
    {
        "text": "This item was put and modified from streamlit!",
        "metadata": None,
        "new_field": "This is a newly added field"
    }
)

# Delete an item from the table:
conn.del_item("new_item")
```

### API variants

The previous examples used the `pandas` API of DynamoDB Connections. In this mode the connection will return Pandas objects (`DataFrame`s or `Series`). You can also use the `raw` API to get the results as standard Python objects (lists and dictionaries).

DynamoDB Connections also lets you access the underlying [DynamoDB Mapping](https://github.com/mrtj/dynamodb-mapping) instance that lets you use your table as if it were a Python Dictionary. For more information, check the [Dictionary API](examples/pages/3_Dictionary_API.py) sample application.

### Table editor

DynamoDB Connections comes with an integration with [Streamlit Data Editor](https://docs.streamlit.io/library/api-reference/data/st.data_editor) called Table Editor. This widget displays all items in your DynamoDB table in an editable way. Your modifications are written to back to the DynamoDB table on the fly, allowing you (or the users of your app) to modify the data in the table in a convenient way. You can try out the table editor in the [Demo application](#demo-application).

Example usage:

```python
import streamlit as st
from dynamodb_connection import DynamoDBConnection

# Create a connection:
conn = st.connection(
    "my_dynamodb_connection", type=DynamoDBConnection, api_type="pandas"
)

# Launch the table editor:
table_editor = DynamoDBTableEditor(conn)
table_editor.edit()
```
