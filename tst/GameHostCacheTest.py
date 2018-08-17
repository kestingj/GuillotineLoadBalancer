import unittest
from GameHostCache import GameHostCache
from mock import patch
import random


class GameHostCacheTest(unittest.TestCase):

    host_names = ["host_1", "host_2", "host_3", "host_4"]
    player_ids = ["Joseph", "Micha", "Peter", "Nick"]

    def setUp(self):
        self.cache = GameHostCache()

    @patch('Hosts.get_hosts')
    @patch('GameHostDao.GameHostDao')
    def testNewGame(self, mock_dao, mock_hosts):

        self.initialize_cache(mock_dao, mock_hosts, self.host_names)

        host_name = self.host_names[0]
        game_id = self.cache.new_game(host_name, self.player_ids)
        mock_dao.new_game.assert_called_with(game_id, host_name, self.player_ids)
        expected_distribution = set()
        expected_distribution.add(game_id)
        self.assertEqual(self.cache.host_distribution[host_name], expected_distribution)
        expected_player_to_games = {}
        for player_id in self.player_ids:
            games = set()
            games.add(game_id)
            expected_player_to_games[player_id] = games
        self.assertEqual(self.cache.player_to_games, expected_player_to_games)
        self.assertEqual(self.cache.game_host_cache[game_id], (host_name, self.player_ids))

    @patch('Hosts.get_hosts')
    def testSyncCacheOnStartup(self, mock_hosts):
        mock_hosts.return_value = self.host_names
        self.cache.sync_hosts()

        expected_distribution = {}
        for host in self.host_names:
            expected_distribution[host] = set()
        self.assertEqual(self.cache.host_distribution, expected_distribution)

    @patch('Hosts.get_hosts')
    @patch('GameHostDao.GameHostDao')
    def testSyncCacheWithNewAndDeadHosts(self, mock_dao, mock_hosts):
        original_hosts = ['stale_host_1', 'stale_host_2', self.host_names[0], self.host_names[1], self.host_names[2]]
        self.initialize_cache(mock_dao, mock_hosts, original_hosts)

        # stale_host_1 and stale_host_2 should be removed. self.host_names[3] should be added
        original_distribution = {
            original_hosts[0] : self.get_game_id_set(self.get_random_game_list(1, original_hosts[0])),
            original_hosts[1] : self.get_game_id_set(self.get_random_game_list(1, original_hosts[1])),
            original_hosts[2] : self.get_game_id_set(self.get_random_game_list(2, original_hosts[2])),
            original_hosts[3] : self.get_game_id_set(self.get_random_game_list(3, original_hosts[3])),
            original_hosts[4] : self.get_game_id_set(self.get_random_game_list(2, original_hosts[4]))
        }

        mock_hosts.return_value = self.host_names

        self.cache.sync_hosts()

        reassigned_games = original_distribution['stale_host_1'] | original_distribution['stale_host_2']
        expected_distribution = {
            self.host_names[0]: original_distribution[self.host_names[0]],
            self.host_names[1]: original_distribution[self.host_names[1]],
            self.host_names[2]: original_distribution[self.host_names[2]],
            self.host_names[3]: reassigned_games
        }

        self.assertEqual(self.cache.host_distribution, expected_distribution)
        self.assertEqual(mock_dao.update_game_host.call_count, 2)
        for game_id in reassigned_games:
            self.assertEqual(self.cache.game_host_cache.get(game_id)[0], self.host_names[3])


    @patch('Hosts.get_hosts')
    @patch('GameHostDao.GameHostDao')
    def testFillCache(self, mock_dao, mock_hosts):

        # Host lists overlap with self.host_names[0...2]
        original_hosts = ['stale_host', self.host_names[0], self.host_names[1], self.host_names[2]]

        self.cache.dao = mock_dao

        scanned_entries = [
            self.get_game_host_entry(original_hosts[0]),
            self.get_game_host_entry(original_hosts[0]),
            self.get_game_host_entry(original_hosts[1]),
            self.get_game_host_entry(original_hosts[1]),
            self.get_game_host_entry(original_hosts[2]),
            self.get_game_host_entry(original_hosts[2]),
            self.get_game_host_entry(original_hosts[3]),
            self.get_game_host_entry(original_hosts[3]),
        ]

        mock_dao.scan_table.return_value = scanned_entries

        mock_hosts.return_value = self.host_names

        self.cache.fill_cache()

        expected_distribution = self.get_expected_distribution(scanned_entries, original_hosts[0], self.host_names[3])
        self.assertEqual(self.cache.host_distribution, expected_distribution)
        self.assertEqual(mock_dao.update_game_host.call_count, 2)

    @patch('Hosts.get_hosts')
    @patch('GameHostDao.GameHostDao')
    def testDeleteGame(self, mock_dao, mock_hosts):
        self.initialize_cache(mock_dao, mock_hosts, self.host_names)

        host_name = self.host_names[0]
        game_id = self.cache.new_game(host_name, self.player_ids)

        self.cache.delete_game(game_id)

        self.assertEqual(self.cache.game_host_cache.get(game_id), None)
        self.assertEqual(self.cache.host_distribution[host_name], set())
        for player_id in self.player_ids:
            self.assertEqual(self.cache.player_to_games.get(player_id), None)

    @patch('GameHostDao.GameHostDao')
    def testDeleteGameIsIdempotent(self, mock_dao):
        self.cache.dao = mock_dao
        self.cache.delete_game('bogus_game')
        mock_dao.delete_game.assert_called()

    @patch('Hosts.get_hosts')
    @patch('GameHostDao.GameHostDao')
    def testFindHostForNewGame(self, mock_dao, mock_hosts):
        self.initialize_cache(mock_dao, mock_hosts, self.host_names)

        original_distribution = {
            self.host_names[0]: self.get_random_game_list(2, self.host_names[0]),
            self.host_names[1]: self.get_random_game_list(3, self.host_names[1]),
            self.host_names[2]: self.get_random_game_list(2, self.host_names[2]),
            self.host_names[3]: self.get_random_game_list(1, self.host_names[3])
        }
        self.cache.host_distribution = original_distribution

        host = self.cache.find_host_with_min_games()

        self.assertEqual(host, self.host_names[3])

    @patch('Hosts.get_hosts')
    @patch('GameHostDao.GameHostDao')
    def testGetGamesForPlayer(self, mock_dao, mock_hosts):
        self.initialize_cache(mock_dao, mock_hosts, self.host_names)
        games = self.get_random_game_list(4, self.host_names[0])

        for player_id in self.player_ids:
            returned_games = self.cache.get_games_for_player(player_id)
            for game_host in returned_games:
                self.assertTrue(game_host in games)

    @patch('Hosts.get_hosts')
    @patch('GameHostDao.GameHostDao')
    def testFindNewHostForGame(self, mock_dao, mock_hosts):
        self.initialize_cache(mock_dao, mock_hosts, self.host_names[:2])
        old_host_name = self.host_names[0]
        new_host_name = self.host_names[1]
        games = self.get_random_game_list(2, old_host_name)
        reassigned_game = games.pop()['gameId']
        other_game = games.pop()['gameId']

        self.cache.reassign_game(reassigned_game)

        self.assertEqual(mock_dao.update_game_host.call_count, 1)
        self.assertEqual(self.cache.game_host_cache.get(reassigned_game)[0], new_host_name)
        self.assertEqual(self.cache.game_host_cache.get(other_game)[0], old_host_name)

        old_host_games = set()
        old_host_games.add(other_game)

        new_host_games = set()
        new_host_games.add(reassigned_game)
        self.assertEqual(self.cache.host_distribution.get(old_host_name), old_host_games)
        self.assertEqual(self.cache.host_distribution.get(new_host_name), new_host_games)

    def get_random_game_list(self, count, host_name):
        games = []
        for i in range(count):
            game_id = self.cache.new_game(host_name, self.player_ids)
            games.append({'gameId': game_id, 'hostName': host_name})
        return games

    def rand_string(self):
        valid_letters = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'
        return ''.join((random.choice(valid_letters) for i in range(10)))

    def get_game_host_entry(self, host_name):
        return {'gameId' : self.rand_string(), 'hostName' : host_name, 'playerIds': self.player_ids}

    def get_expected_distribution(self, entries, host_to_remove, host_to_add):
        distribution = {}
        for entry in entries:
            game_id = entry['gameId']
            host_name = entry['hostName']
            if host_name == host_to_remove:
                host_name = host_to_add
            if host_name in distribution.keys():
                distribution[host_name].add(game_id)
            else:
                games = set()
                games.add(game_id)
                distribution[host_name] = games

        return distribution

    def initialize_cache(self, mock_dao, mock_hosts, host_list):
        self.cache.dao = mock_dao
        mock_hosts.return_value = host_list
        self.cache.sync_hosts()

    def get_game_id_set(self, game_host_list):
        game_id_list = set()
        for game_host in game_host_list:
            game_id_list.add(game_host['gameId'])

        return game_id_list

if __name__ == '__main__':
    unittest.main()