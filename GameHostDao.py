import boto3


class GameHostDao:

    def __init__(self):
        dynamodb = boto3.resource('dynamodb', region_name='us-west-2')
        self.table = dynamodb.Table('game-host-table')

    def new_game(self, game_id, host_name, player_ids):
        self.table.put_item(
            Item = {
                'gameId' : game_id,
                'hostName' : host_name,
                'playerIds' : player_ids,
                'status': 'Active'
            }
        )

    def update_game_host(self, game_id, new_host_id):
        self.table.update_item(
            Key={
                'gameId': game_id
            },
            UpdateExpression="set hostName = :h",
            ExpressionAttributeValues={
                ':h': new_host_id
            }
        )

    def finish_game(self, game_id, s3Bucket, s3Key):
        key = {'gameId': game_id}

        updates = {'status': {'Value': 'Completed', 'Action': 'PUT'},
                   's3Bucket': {'Value': s3Bucket, 'Action': 'PUT'},
                   's3Key': {'Value': s3Key, 'Action': 'PUT'},
                   'hostName': {'Action': 'REMOVE'}}

        self.table.update_item(Key=key, AttributeUpdates=updates)

    def scan_table(self):
        # TODO update to only scan for Active games - need to add GSI on status field
        scan_result = self.table.scan()
        entries = scan_result['Items']

        while 'LastEvaluatedKey' in scan_result:
            scan_result = self.table.scan(ExclusiveStartKey=scan_result['LastEvaluatedKey'])
            entries.extend(scan_result['Items'])

        return entries

