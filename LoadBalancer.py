from flask import Flask, jsonify, request
from GameHostCache import GameHostCache

app = Flask(__name__)
cache = GameHostCache()

@app.route('/games', methods=['POST'])
def start_new_game():
    host_name = cache.find_host_with_min_games()
    new_game_message = request.get_json()
    player_ids = new_game_message['playerIds']
    game_id = cache.new_game(host_name, player_ids)
    return jsonify({'gameId':game_id, 'hostName':host_name})

@app.route('/players/<string:player_id>/games', methods=['GET'])
def get_games_for_player(player_id):
    games = cache.get_games_for_player(player_id)
    return jsonify({'games': games})

@app.route('games/<string:game_id>', methods=['DELETE'])
def delete_game(game_id):
    message = request.get_json()
    cache.delete_game(game_id, message['s3Bucket', 's3Key'])


