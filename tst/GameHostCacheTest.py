import unittest
from GameHostCache import GameHostCache
from mock import MagicMock
from mock import patch


class GameHostTest(unittest.TestCase):

    game_id = "game_id"
    host_name = "hostess with the mostest"

    def setUp(self):
        self.cache = GameHostCache()

    @patch('Hosts.get_hosts')
    @patch('GameHostDao.GameHostDao')
    def testNewGame(self, mock_dao, mock_hosts):
        self.cache.dao = mock_dao
        mock_hosts.return_value = [self.host_name]
        self.cache.fill_cache()
        self.cache.new_game(self.game_id, self.host_name)
        mock_dao.new_game.assert_called_with(self.game_id, self.host_name)
        self.assertEqual(self.cache.distribution[self.host_name], 1)



    @patch('Hosts.get_hosts')
    @patch('GameHostDao.GameHostDao')
    def testGetHost(self, mock_dao, mock_hosts):
        self.cache.dao = mock_dao
        mock_hosts.return_value = [self.host_name]
        self.cache.fill_cache()
        self.cache.new_game(self.game_id, self.host_name)
        self.assertEqual(self.cache.get_host(self.game_id), self.host_name)

    @patch('GameHostDao.GameHostDao')
    def testFillCache(self, mock_dao):


    def testDeleteGame(self):
        pass

    def testFindHostForNewGame(self):
        pass

if __name__ == '__main__':
    unittest.main()