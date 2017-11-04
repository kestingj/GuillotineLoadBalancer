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
                'playerIds' : player_ids
            }
        )

    def update_game_host(self, game_id, new_host_id):
        self.table.update_item(
            Key={
                'gameId': game_id
            },
            UpdaterExpression="set hostName = :h"
        ,
            ExpressionAttributeValues={
                ':h': new_host_id
            }
        )

    def delete_game(self, game_id):
        self.table.delete_item(gameId=game_id)

    def scan_table(self):
        scan_result = self.table.scan()
        entries = scan_result['Items']

        while 'LastEvaluatedKey' in scan_result:
            scan_result = self.table.scan(ExclusiveStartKey=scan_result['LastEvaluatedKey'])
            entries.extend(scan_result['Items'])

        return entries

