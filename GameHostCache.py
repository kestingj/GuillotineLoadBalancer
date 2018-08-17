from GameHostDao import GameHostDao
import Hosts
import sys
import uuid


class GameHostCache:

    def __init__(self):
        self.game_host_cache = {}
        self.host_distribution = {}
        self.player_to_games = {}
        self.dao = GameHostDao()

    # Starts a new game with a random id and the specified host_name and playerIds
    def new_game(self, host_name, player_ids):
        game_id = str(uuid.uuid4())
        self.dao.new_game(game_id, host_name, player_ids)
        self.game_host_cache[game_id] = (host_name, player_ids)
        self.host_distribution[host_name].add(game_id)
        self.__associate_game_with_players__(game_id, player_ids)
        return game_id

    def reassign_game(self, game_id):
        host_name = self.find_host_with_min_games()
        self.__update_game_host__(game_id, host_name)
        return host_name

    def get_games_for_player(self, player_id):
        game_host_list = []
        for game_id in self.player_to_games[player_id]:
            host_name = self.game_host_cache[game_id][0]
            game_host_list.append({'gameId': game_id, 'hostName': host_name})

        return game_host_list

    def fill_cache(self):
        self.sync_hosts()

        entries = self.dao.scan_table()
        orphaned_games = {}
        for entry in entries:
            game_id = entry['gameId']
            host_name = entry['hostName']
            player_ids = entry['playerIds']
            self.__associate_game_with_players__(game_id, player_ids)
            if host_name in self.host_distribution:
                self.game_host_cache[game_id] = (host_name, player_ids)
                self.host_distribution[host_name].add(game_id)
            else:  # game's host is no longer active
                orphaned_games[game_id] = player_ids

        # Find new hosts for orphaned games
        for game_id, player_ids in orphaned_games.items():
            new_host = self.find_host_with_min_games()
            self.dao.update_game_host(game_id, new_host)
            self.game_host_cache[game_id] = (new_host, player_ids)
            self.host_distribution[new_host].add(game_id)

    # TODO: Need a worker that refreshes the cache at a specified interval
    def sync_hosts(self):
        # Get canonical list of live hosts
        live_hosts = Hosts.get_hosts()
        # Get list of current hosts according to the load-balancer
        hosts_with_games = self.host_distribution.keys()
        # Get list of hosts to add to load-balancer
        hosts_to_initialize = live_hosts - hosts_with_games
        # Get list of hosts that are no longer active and need to be removed from the load-balancer
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
            self.__update_game_host__(game, new_host)

    def delete_game(self, game_id):
        self.dao.delete_game(game_id)

        # Maintain idempotency of deletes
        if game_id in self.game_host_cache:
            host_name, player_ids = self.game_host_cache.pop(game_id)
            if host_name is not None:
                self.host_distribution[host_name].discard(game_id)
                for player_id in player_ids:
                    self.player_to_games[player_id].discard(game_id)
                    if len(self.player_to_games[player_id]) == 0:
                        # Cleanup player if they have 0 games left. This simplifies the state of a player with no games
                        # and a new player to a single state.
                        del self.player_to_games[player_id]

    # TODO: Should be called by background thread that pings all Backend hosts for health
    def __update_game_host__(self, game_id, new_host_id):
        existing_host, player_ids = self.game_host_cache.pop(game_id)
        self.dao.update_game_host(game_id, new_host_id)
        self.game_host_cache[game_id] = (new_host_id, player_ids)
        if existing_host in self.host_distribution:
            self.host_distribution[existing_host].discard(game_id)
        self.host_distribution[new_host_id].add(game_id)

    # Returns host with the minimum number of ongoing games
    def find_host_with_min_games(self):
        min_games = sys.maxsize
        min_host = None
        for entry in self.host_distribution.items():
            host_name = entry[0]
            num_games = len(entry[1])
            if num_games < min_games:
                min_games = num_games
                min_host = host_name
        return min_host

    def __associate_game_with_players__(self, game_id, player_ids):
        for player_id in player_ids:
            if player_id not in self.player_to_games:
                # New player
                self.player_to_games[player_id] = set()

            self.player_to_games[player_id].add(game_id)
