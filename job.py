class Job():
	__REQUIRED_OPTS = ['name', 'map_func', 'reduce_func', 'input_dir', 'output_dir', 'mapper_count', 'reducer_count']
	__OPTIONAL_OPTS = { 'working_dir' : '', 'sort_key_provider': None } 
	
	def __init__(self, **kwargs):
		self.validate_opts(kwargs)
		self.opts = dict(list(self.__OPTIONAL_OPTS.items()) + list(kwargs.items()))
		for opt in self.opts:
			if opt in self.__REQUIRED_OPTS or opt in self.__OPTIONAL_OPTS:
				setattr(self, opt, self.opts[opt])
		
	
	def validate_opts(self, opts):
		keys = opts.keys()
		for opt in self.__REQUIRED_OPTS:
			if opt not in keys or not opts[opt]:
				raise Exception("Missing required argument: " + opt)
	
			
				
		
