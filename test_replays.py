import pytest
import requests

from replays import ReplayData, Difficulty

class TestCaseWc3Stats:
	def __init__(self, replay_id, map_file, difficulty, continues, win):
		self.replay_id = replay_id
		self.map_file = map_file
		self.difficulty = difficulty
		self.continues = continues
		self.win = win

@pytest.mark.parametrize("test_case", [
	TestCaseWc3Stats(
		99971,
		"Impossible.Bosses.v1.11.4-nobnet",
		Difficulty.N,
		continues=True,
		win=False
	),
	TestCaseWc3Stats(
		100502,
		"Impossible.Bosses.v1.11.4-nobnet",
		Difficulty.N,
		continues=True,
		win=True
	),
	TestCaseWc3Stats(
		100713,
		"Impossible.Bosses.v1.11.5-no-bnet",
		Difficulty.N,
		continues=True,
		win=True
	),
	TestCaseWc3Stats(
		100995,
		"Impossible.Bosses.v1.11.5-no-bnet",
		Difficulty.H,
		continues=True,
		win=True
	)
])
def test_wc3stats_replay(test_case):
	r = requests.get("https://api.wc3stats.com/replays/{}".format(test_case.replay_id))
	assert r.status_code == 200

	data = ReplayData(r.json())
	assert data.id == test_case.replay_id
	assert data.map == test_case.map_file
	assert data.difficulty == test_case.difficulty
	assert data.continues == test_case.continues
	assert data.win == test_case.win
