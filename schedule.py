import itertools

class Schedule:
	
	def __init__(self, param):
		self.param = param

	def schedule(self):
		param = sorted(self.param.items())
		name, values = [name[0] for name in param], [value[1] for value in param]
		for values in itertools.product(*values):
			yield list(zip(name, values))
	
if __name__ == '__main__':
	param = {'param_one': (1, 2, 3, 4), 'param_two': (4, 5, 6), 'param_three': (7, 8, 9)}
	schedule = Schedule(param)
	for i in schedule.schedule():
		print(i)
