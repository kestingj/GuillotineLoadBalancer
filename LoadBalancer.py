from flask import Flask, jsonify, request
from GameHostCache import GameHostCache
import requests

app = Flask(__name__)
cache = GameHostCache()

@app.route('/games', methods=['POST'])
def start_new_game():
    host = cache.find_host_with_min_games()
    new_game_message = request.get_json()
    put_request = requests.post(host + '/games', json=new_game_message)
    game_id = put_request.json()['gameId']
    cache.new_game(game_id)

@app.route('/games/<string:game_id>/<string:player_id>', methods=['GET'])
def get_player_state(game_id, player_id):
    host = cache.get_host(game_id)
    get_request = requests.get(host + '/games/' + game_id + '/' + player_id)
    return jsonify(get_request.json())

@app.route('/games/<string:game_id>/<string:player_id>', methods=['PUT'])
def play(game_id, player_id):
    host = cache.get_host(game_id)
    play_message = request.get_json()
    put_request = requests.put(host + '/games/' + game_id + '/' + player_id, data=play_message)
    return jsonify(put_request.json())

@app.route('/players/<string:player_id>/games', methods=['GET'])
def get_games_for_player(player_id):

