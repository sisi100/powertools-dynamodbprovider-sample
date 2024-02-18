import boto3
from aws_lambda_powertools.utilities import parameters
from moto import mock_aws


# テーブルを作成する
def create_table(dynamodb, table_name: str):
    table = dynamodb.create_table(
        TableName=table_name,
        KeySchema=[
            {"AttributeName": "pk", "KeyType": "HASH"},  # パーティションキー
            {"AttributeName": "sk", "KeyType": "RANGE"},  # ソートキー
        ],
        AttributeDefinitions=[
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "sk", "AttributeType": "S"},
        ],
        ProvisionedThroughput={"ReadCapacityUnits": 1, "WriteCapacityUnits": 1},
    )
    return table


# テストデータを投入する
def put_test_data(table, test_data):
    for item in test_data:
        table.put_item(Item=item)


def main():
    table_name = "user_config_table"
    test_data = [
        {"pk": "User_A", "sk": "param_1", "value": "UserAのパラメーター１だよ"},
        {"pk": "User_A", "sk": "param_2", "value": "UserAのパラメーター２だよ"},
        {"pk": "User_B", "sk": "param_1", "value": "UserBのパラメーター１だよ"},
    ]

    with mock_aws():
        dynamodb = boto3.resource("dynamodb")
        table = create_table(dynamodb, table_name)
        put_test_data(table, test_data)

        dynamodb_provider = parameters.DynamoDBProvider(
            table_name=table_name, key_attr="pk", sort_attr="sk", value_attr="value"
        )

        print("########## User_Aのパラメーターを取得")
        user_configs = dynamodb_provider.get_multiple("User_A")
        for parameter, value in user_configs.items():
            print(f"{parameter}: {value}")

        print("########## User_Bのパラメーターを取得")
        user_configs = dynamodb_provider.get_multiple("User_B")
        for parameter, value in user_configs.items():
            print(f"{parameter}: {value}")

        # (おまけ)boto3を使って直接DynamoDBにアクセスする場合
        print("########## User_Aのパラメーターを取得")
        response = table.query(
            KeyConditionExpression="pk = :pk_value",
            ExpressionAttributeValues={
                ":pk_value": "User_A",
            },
        )
        items = response.get("Items")
        for item in items:
            parameter = item["sk"]
            value = item["value"]
            print(f"{parameter}: {value}")


if __name__ == "__main__":
    main()
