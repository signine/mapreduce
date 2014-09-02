from mapreduce.job import Job

class JobBuilder():
	def __init__(self):
		self.args = {}

	def set_name(self, name):
		self.args['name'] = name
		return self
	
	def set_mapper(self, map_func):
		self.args['map_func'] = map_func
		return self
	
	def set_reducer(self, reduce_func):
		self.args['reduce_func'] = reduce_func
		return self
	
	def set_input_dir(self, dir):
		self.args['input_dir'] = dir
		return self
	
	def set_working_dir(self, dir):
		self.args['working_dir'] = dir
		return self 
	
	def set_output_dir(self, dir):
		self.args['output_dir'] = dir
		return self
	
	def set_mapper_count(self, count):
		self.args['mapper_count'] = count
		return self
		
	def set_reducer_count(self, count):
		self.args['reducer_count'] = count
		return self
	
	def set_sort_key_provider(self, func):
		self.args['sort_key_provider'] = func
		return self
	
	def build(self):
		return Job(**self.args)
		
	