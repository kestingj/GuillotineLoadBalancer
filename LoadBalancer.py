from flask import Flask, jsonify, request
from GameHostDao import GameHostDao

app = Flask(__name__)
dao = GameHostDao()

@app.route('/games', methods=['POST'])
def startNewGame():
    player_ids = request.get_json()['playerIds']
    starting_player = request.get_json()['startingPlayer']
    game_id = game_manager.createNewGame(player_ids, starting_player)
    return jsonify({'gameCount' : len(game_manager.games), 'gameId' : str(game_id)})

@app.route('/games/<string:game_id>/<string:player_id>', methods=['GET'])
def getPlayerState(game_id, player_id):
    player_state = game_manager.getPlayerState(game_id, player_id)
    return jsonify(player_state)

@app.route('/games/<string:game_id>/<string:player_id>', methods=['PUT'])
def play(game_id, player_id):
    play_list = request.get_json()['play']
    play_set = set()
    for card in play_list:
        play_set.add(Card(card['rank'], card['suit']))
    game_manager.playHand(game_id, player_id, play_set)
    return jsonify(game_manager.getPlayerState(game_id, player_id)['hand'])