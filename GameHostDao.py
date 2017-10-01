import boto3

client = boto3.client('dynamodb')
client.

class GameHostDao:

    def __init__(self):
        dynamodb = boto3.resource('dynamodb', region_name='us-west-2')
        self.table = dynamodb.Table('game-host-table')

    def get_host(self, game_id):
        response = self.table.get_item(
            Key = {
                'gameId': game_id
            }
        )
        hostName = response['hostName']
        return hostName

    def new_game(self, game_id, host_name):
        self.table.put_item(
            Item = {
                'gameId' : game_id,
                'hostName' : host_name
            }
        )
