from GameHostDao import GameHostDao
import Hosts
import sys


class GameHostCache:
    def __init__(self):
        self.game_host_cache = {}
        self.host_distribution = {}
        self.player_to_games = {}
        self.dao = GameHostDao()

    def get_host(self, game_id):
        return self.game_host_cache[game_id]

    def new_game(self, game_id, host_name, player_ids):
        self.dao.new_game(game_id, host_name, player_ids)
        self.game_host_cache[game_id] = (host_name, player_ids)
        self.host_distribution[host_name].add(game_id)
        self.associate_game_with_players(game_id, player_ids)

    def fill_cache(self):
        self.sync_hosts()

        entries = self.dao.scan_table()
        orphaned_games = set()
        for entry in entries:
            game_id = entry['gameId']
            host_name = entry['hostName']
            player_ids = entry['playerIds']
            self.associate_game_with_players(game_id, player_ids)
            if host_name in self.host_distribution:
                self.game_host_cache[game_id] = (host_name, player_ids)
                self.host_distribution[host_name].add(game_id)
            else: # game's host is no longer active
                orphaned_games.add(game_id)

        # Find new hosts for orphaned games
        for game_id in orphaned_games:
            new_host = self.find_host_with_min_games()
            self.update_game_host(game_id, new_host)

    def delete_game(self, game_id):
        self.dao.delete_game(game_id)
        host_name, player_ids = self.game_host_cache.pop(game_id)
        if host_name is not None:
            self.host_distribution[host_name].discard(game_id)
            for player_id in player_ids:
                self.player_to_games[player_id].discard(game_id)
                if len(self.player_to_games[player_id]) == 0:
                    del self.player_to_games[player_id]

    def sync_hosts(self):
        # Get canonical list of live hosts
        live_hosts = Hosts.get_hosts()
        # Get list of current hosts according to the load-balancer
        hosts_with_games = self.host_distribution.keys()
        # Get list of hosts to add to load-balancer
        hosts_to_initialize = live_hosts - hosts_with_games
        # Get lit of hosts that are no longer active and need to be removed from the load-balancer
        hosts_to_kill = hosts_with_games - live_hosts
        # Initialize new hosts
        for host in hosts_to_initialize:
            self.host_distribution[host] = set()
        # Find games that need to be redistributed
        orphaned_games = set()
        for host in hosts_to_kill:
            games = self.host_distribution.pop(host)
            orphaned_games = orphaned_games.union(games)
        # Redistribute games
        for game in orphaned_games:
            new_host = self.find_host_with_min_games()
            self.update_game_host(game, new_host)

    # Returns host with the minimum number of ongoing games
    def find_host_with_min_games(self):
        min_games = sys.maxsize
        min_host = None
        for entry in self.host_distribution.items():
            host_name = entry[0]
            num_games = len(entry[1])
            if num_games <= min_games:
                min_games = num_games
                min_host = host_name
        return min_host

    def update_game_host(self, game_id, new_host_id):
        existing_host, player_ids = self.game_host_cache.pop(game_id)
        self.dao.update_game_host(game_id, new_host_id)
        self.game_host_cache[game_id] = (new_host_id, player_ids)
        if existing_host in self.host_distribution:
            self.host_distribution[existing_host].discard(game_id)
        self.host_distribution[new_host_id].add(game_id)

    def associate_game_with_players(self, game_id, player_ids):
        for player_id in player_ids:
            if player_id in self.player_to_games:
                self.player_to_games[player_id].add(game_id)
            else:
                self.player_to_games[player_id] = set(game_id)




