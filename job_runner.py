from multiprocessing import Pool, current_process
from os import path, walk
import simplejson as json

class JobRunner():
	def run(self, job):
		print("Starting job: ", job.name)
		mapper = Mapper(job)
		interm_data = mapper.run()
		interm_data = self.__sort(job, interm_data)
		reducer = Reducer(job)
		return reducer.run(interm_data)
	
	def __sort(self, job, data):
		if not job.sort_key_provider:
			key_provider = lambda x: x[0]
		else:
			key_provider = lambda x: job.sort_key_provider(x[0])
		return sorted(data, key=key_provider)
	

class Mapper():
	"""
		All keys must be hashable
	"""
	
	def __init__(self, job):
		self.job = job
	
	def map_wrapper(self, file):
		print("Mapping file: ", path.basename(file))
		result = {}
		for key, value in self.job.map_func(file):
			if key in result:
				result[key].append(value)
			else:
				result[key] = [value]
		return result
	
	def __flatten(self, l):
		return [ item for sublist in l for item in sublist ]
	
	def __merge_results(self, results):
		merged = {}
		keys = set(self.__flatten([ r.keys() for r in results ]))
		for k in keys:
			for r in results:
				if k in r:
					if k in merged:
						merged[k] += r[k]
					else:
						merged[k] = r[k]
		return merged
	
	def __get_input_files(self):
		basedir = self.job.input_dir
		files = walk(basedir).__next__()[2]
		return [ path.join(basedir, f) for f in files ]
					
		
	def run(self):
		""" 
			Returns a list of tuples [(key, value)...]
		"""
		pool = Pool(processes=self.job.mapper_count,)
		input_files = self.__get_input_files()
		print("Total files: ", len(input_files))
		results = pool.map(self.map_wrapper, iter(input_files))
		results = self.__merge_results(results)
		self.results = list(results.items())
		return self.results


class Reducer():

	def __init__(self, job):
		self.job = job
	
	def __chunks(self, l, n):
		for i in range(0, len(l), n):
			yield l[i:i+n]
			
	def reduce_wrapper(self, data):
		# TODO: make name for meaningful
		filename = str(current_process().pid) + ".txt"
		results = []
		with open(path.join(self.job.output_dir, filename), 'w') as f:
			for d in self.job.reduce_func(data):
				results.append(d)
				f.write(json.dumps(d) + "\n")
		return results
	
	def run(self, data):
    """
       Returns list of results from each reducer [[Reducer 1], [Reducer 2], ...]
    """
		pool = Pool(processes=self.job.reducer_count,)
		print("Running reduce on ", len(data), " records")
		chunk_size = int(len(data) / self.job.reducer_count)
		self.results = pool.map(self.reduce_wrapper, self.__chunks(data, chunk_size))
		return self.results
