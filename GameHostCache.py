from GameHostDao import GameHostDao
import Hosts
import sys

class GameHostCache:
    def __init__(self):
        self.cache = {}
        self.distribution = {}
        self.dao = GameHostDao()

    def get_host(self, game_id):
        return self.cache[game_id]

    def new_game(self, game_id, host_name):
        self.dao.new_game(game_id, host_name)
        self.cache[game_id] = host_name
        self.distribution[host_name] += 1

    def fill_cache(self):
        entries = self.dao.scan_table()
        for entry in entries:
            game_id = entry['gameId']
            host_name = entry['hostName']
            self.cache[game_id] = host_name
            if host_name in self.distribution:
                self.distribution[host_name] += 1
            else:
                self.distribution[host_name] = 1

        # Add all hosts with no games currently assigned to them
        for host_name in Hosts.get_hosts():
            if host_name not in self.distribution:
                self.distribution[host_name] = 0

    def delete_game(self, game_id):
        self.dao.delete_game(game_id)
        host_name = self.cache.pop(game_id, None)
        if host_name is not None:
            self.distribution[host_name] -= 1


    # Returns host with the minimum number of ongoing games
    def find_host_for_new_game(self):
        min_games = sys.maxsize
        min_host = None
        for entry in self.distribution.items():
            host_name = entry[0]
            num_games = entry[1]
            if num_games < min_games:
                min_games = num_games
                min_host = host_name
        return min_host





