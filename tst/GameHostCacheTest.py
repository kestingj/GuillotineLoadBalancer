import unittest
from GameHostCache import GameHostCache
from mock import patch
import random


class GameHostCacheTest(unittest.TestCase):

    game_id = "game_id"
    host_names = ["host_1", "host_2", "host_3", "host_4"]

    def setUp(self):
        self.cache = GameHostCache()

    @patch('Hosts.get_hosts')
    @patch('GameHostDao.GameHostDao')
    def testNewGame(self, mock_dao, mock_hosts):
        host_name = self.host_names[0]
        self.cache.dao = mock_dao
        mock_hosts.return_value = [host_name]
        self.cache.fill_cache()
        self.cache.new_game(self.game_id, host_name)
        mock_dao.new_game.assert_called_with(self.game_id, host_name)
        expected_distribution = set()
        expected_distribution.add(self.game_id)
        self.assertEqual(self.cache.distribution[host_name], expected_distribution)

    @patch('Hosts.get_hosts')
    @patch('GameHostDao.GameHostDao')
    def testGetHost(self, mock_dao, mock_hosts):
        host_name = self.host_names[0]
        self.cache.dao = mock_dao
        mock_hosts.return_value = [host_name]
        self.cache.fill_cache()
        self.cache.new_game(self.game_id, host_name)
        self.assertEqual(self.cache.get_host(self.game_id), host_name)

    @patch('Hosts.get_hosts')
    def testSyncCacheOnStartup(self, mock_hosts):
        mock_hosts.return_value = self.host_names
        self.cache.sync_hosts()

        expected_distribution = {}
        for host in self.host_names:
            expected_distribution[host] = set()
        self.assertEqual(self.cache.distribution, expected_distribution)

    @patch('Hosts.get_hosts')
    @patch('GameHostDao.GameHostDao')
    def testSyncCacheWithNewAndDeadHosts(self, mock_dao, mock_hosts):
        mock_hosts.return_value = self.host_names
        self.cache.dao = mock_dao

        # stale_host_1 and stale_host_2 should be removed. self.host_names[3] should be added
        original_distribution = {
            'stale_host_1' : self.getRandomGameSet(1),
            'stale_host_2' : self.getRandomGameSet(1),
            self.host_names[0] : self.getRandomGameSet(2),
            self.host_names[1] : self.getRandomGameSet(3),
            self.host_names[2] : self.getRandomGameSet(2)
        }
        self.cache.distribution = original_distribution.copy()

        self.cache.sync_hosts()

        reassigned_games = original_distribution['stale_host_1'] | original_distribution['stale_host_2']
        expected_distribution = {
            self.host_names[0]: original_distribution[self.host_names[0]],
            self.host_names[1]: original_distribution[self.host_names[1]],
            self.host_names[2]: original_distribution[self.host_names[2]],
            self.host_names[3]: reassigned_games
        }

        self.assertEqual(self.cache.distribution, expected_distribution)
        self.assertEqual(mock_dao.new_game.call_count, 2)


    @patch('Hosts.get_hosts')
    @patch('GameHostDao.GameHostDao')
    def testFillCache(self, mock_dao, mock_hosts):
        scanned_entries = [
            self.get_game_host_entry('stale_host'),
            self.get_game_host_entry('stale_host'),
            self.get_game_host_entry(self.host_names[0]),
            self.get_game_host_entry(self.host_names[0]),
            self.get_game_host_entry(self.host_names[1]),
            self.get_game_host_entry(self.host_names[1]),
            self.get_game_host_entry(self.host_names[2]),
            self.get_game_host_entry(self.host_names[2]),
        ]
        mock_dao.scan_table.return_value = scanned_entries
        self.cache.dao = mock_dao
        mock_hosts.return_value = self.host_names

        self.cache.fill_cache()

        expected_distribution = self.get_expected_distribution(scanned_entries, 'stale_host', self.host_names[3])
        self.assertEqual(self.cache.distribution, expected_distribution)
        self.assertEqual(mock_dao.new_game.call_count, 2)

    @patch('Hosts.get_hosts')
    @patch('GameHostDao.GameHostDao')
    def testDeleteGame(self, mock_dao, mock_hosts):
        host_name = self.host_names[0]
        self.cache.dao = mock_dao
        mock_hosts.return_value = [host_name]
        self.cache.fill_cache()
        self.cache.new_game(self.game_id, host_name)

        self.cache.delete_game(self.game_id)

        self.assertRaises(KeyError, self.cache.get_host, self.game_id)
        self.assertEqual(self.cache.distribution[host_name], set())

    @patch('GameHostDao.GameHostDao')
    def testDeleteGameIsIdempotent(self, mock_dao):
        self.cache.dao = mock_dao
        self.cache.delete_game('bogus_game')
        mock_dao.delete_game.assert_called()

    def testFindHostForNewGame(self):
        original_distribution = {
            self.host_names[0]: self.getRandomGameSet(2),
            self.host_names[1]: self.getRandomGameSet(3),
            self.host_names[2]: self.getRandomGameSet(2),
            self.host_names[3]: self.getRandomGameSet(1)
        }
        self.cache.distribution = original_distribution

        host = self.cache.find_host_with_min_games()

        self.assertEqual(host, self.host_names[3])


    def getRandomGameSet(self, count):
        games = set()
        for i in range(count):
            games.add(self.randString())
        return games

    def randString(self):
        valid_letters = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'
        return ''.join((random.choice(valid_letters) for i in range(10)))

    def get_game_host_entry(self, host_name):
        return {'gameId' : self.randString(), 'hostName' : host_name}

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


if __name__ == '__main__':
    unittest.main()