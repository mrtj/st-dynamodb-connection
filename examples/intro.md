# Streamlit DynamoDB Connection

Streamlit DynamoDB Connection is a Python library that connects your Streamlit application to an [Amazon DynamoDB](https://aws.amazon.com/dynamodb/) table. This integration allows you to read and write persistent data in your table as simple as it were a Python dictionary. You can also visualize and edit the data from the user interface of your the application using an intuitive table widget.

[Streamlit](https://streamlit.io) is an open-source Python library that allows developers to create web applications for machine learning and data science projects quickly and easily.

[Amazon DynamoDB](https://aws.amazon.com/dynamodb/) is a fully managed NoSQL database service provided by [Amazon Web Services (AWS)](http://aws.amazon.com).

The primary method of data access in DynamoDB involves retrieving schema-less items by specifying their key, a process quite similar to the Python dictionary API. This library builds on the [dynamodb-mapping](https://github.com/mrtj/dynamodb-mapping) Python package that implements the dictionary interface over a DynamoDB table.

This demo application showcases the features of the package: how to create a connection, the different API flavours, as well as the Editable Table feature. For more information about Streamlit DynamoDB Connection visit the [project page on GitHub](https://github.com/mrtj/st-dynamodb-connection).
