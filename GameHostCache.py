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
        self.distribution[host_name].add(game_id)

    def fill_cache(self):
        self.sync_hosts()

        entries = self.dao.scan_table()
        orphaned_games = set()
        for entry in entries:
            game_id = entry['gameId']
            host_name = entry['hostName']
            if host_name in self.distribution:
                self.cache[game_id] = host_name
                self.distribution[host_name].add(game_id)
            else: # game's host is no longer active
                orphaned_games.add(game_id)

        # Find new hosts for orphaned games
        for game_id in orphaned_games:
            new_host = self.find_host_with_min_games()
            self.new_game(game_id, new_host)

    def delete_game(self, game_id):
        self.dao.delete_game(game_id)
        host_name = self.cache.pop(game_id, None)
        if host_name is not None:
            self.distribution[host_name].remove(game_id)

    def sync_hosts(self):
        # Get canonical list of live hosts
        live_hosts = Hosts.get_hosts()
        # Get list of current hosts according to the load-balancer
        hosts_with_games = self.distribution.keys()
        # Get list of hosts to add to load-balancer
        hosts_to_initialize = live_hosts - hosts_with_games
        # Get lit of hosts that are no longer active and need to be removed from the load-balancer
        hosts_to_kill = hosts_with_games - live_hosts
        # Initialize new hosts
        for host in hosts_to_initialize:
            self.distribution[host] = set()
        # Find games that need to be redistributed
        orphaned_games = set()
        for host in hosts_to_kill:
            games = self.distribution.pop(host)
            orphaned_games = orphaned_games.union(games)
        # Redistribute games
        for game in orphaned_games:
            new_host = self.find_host_with_min_games()
            self.new_game(game, new_host)

    # Returns host with the minimum number of ongoing games
    def find_host_with_min_games(self):
        min_games = sys.maxsize
        min_host = None
        for entry in self.distribution.items():
            host_name = entry[0]
            num_games = len(entry[1])
            if num_games <= min_games:
                min_games = num_games
                min_host = host_name
        return min_host





