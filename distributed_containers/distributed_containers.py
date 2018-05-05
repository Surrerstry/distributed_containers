__author__ = 'Surrerstry'
__version__ = '0.41'
__website__ = 'surrerstry.pl'


class distributed_container(object):
	"""
	Conceptual library written for pypy-stm that target is to get more from
	multithreading possibilities of pypy-stm.

	Using of this library on standard implementation of Python is pointless probably.

	>>> # DOCTESTS:

	>>> # 'TESTS_OF:self.count'
	>>> from random import randint
	>>> input_container = [randint(5,15) for x in range(1000)]
	>>> container = distributed_container(input_container, 16)
	>>> multithreading_result = container.count(10)
	>>> one_thread_result = input_container.count(10)
	>>> multithreading_result == one_thread_result
	True
	>>> input_container = [randint(5,15) for x in range(1000)]
	>>> container = distributed_container(input_container, 16, 4)
	>>> multithreading_result = container.count(10)
	>>> one_thread_result = input_container.count(10)
	>>> multithreading_result == one_thread_result
	True

	>>> # 'TESTS_OF:self.indexes'
	>>> input_container = [0,1,2,3,4,5,6,7,8,9,10]*99
	>>> container = distributed_container(input_container, 64)
	>>> container.indexes(10)
	[10, 21, 32, 43, 54, 65, 76, 87, 98, 109, 120, 131, 142, 153, 164, 175, 186, 197, 208, 219, 230, \
241, 252, 263, 274, 285, 296, 307, 318, 329, 340, 351, 362, 373, 384, 395, 406, 417, 428, 439, \
450, 461, 472, 483, 494, 505, 516, 527, 538, 549, 560, 571, 582, 593, 604, 615, 626, 637, 648, \
659, 670, 681, 692, 703, 714, 725, 736, 747, 758, 769, 780, 791, 802, 813, 824, 835, 846, 857, \
868, 879, 890, 901, 912, 923, 934, 945, 956, 967, 978, 989, 1000, 1011, 1022, 1033, 1044, 1055, \
1066, 1077, 1088]
	>>> input_container = [0,1,2,3,4,5,6,7,8,9,10]*99
	>>> container = distributed_container(input_container, 64, 4)
	>>> container.indexes(10)
	[10, 21, 32, 43, 54, 65, 76, 87, 98, 109, 120, 131, 142, 153, 164, 175, 186, 197, 208, 219, 230, \
241, 252, 263, 274, 285, 296, 307, 318, 329, 340, 351, 362, 373, 384, 395, 406, 417, 428, 439, \
450, 461, 472, 483, 494, 505, 516, 527, 538, 549, 560, 571, 582, 593, 604, 615, 626, 637, 648, \
659, 670, 681, 692, 703, 714, 725, 736, 747, 758, 769, 780, 791, 802, 813, 824, 835, 846, 857, \
868, 879, 890, 901, 912, 923, 934, 945, 956, 967, 978, 989, 1000, 1011, 1022, 1033, 1044, 1055, \
1066, 1077, 1088]

	>>> # 'TESTS_OF:self.remove_all'
	>>> dc = distributed_container([1, 2, 3, 4, 1, 2, 1], 2)
	>>> dc.remove_all([1,2])
	[3, 4]
	>>> dc = distributed_container([1, 2, 3, 4, 1, 2, 1], 2, 4)
	>>> dc.remove_all([1,2])
	[3, 4]

	>>> # 'TESTS_OF:self.remove_all'
	>>> input_container = [8, 7, 6, 5, 4, 3, 2, 1]
	>>> dc = distributed_container(input_container, 4)
	>>> dc.sort()
	[1, 2, 3, 4, 5, 6, 7, 8]
	>>> input_container = [88, 8, 7, 6, 5, 4, 3, 2, 0, 1, 111]
	>>> dc = distributed_container(input_container, 4)
	>>> dc.sort()
	[0, 1, 2, 3, 4, 5, 6, 7, 8, 88, 111]
	>>> input_container = [88, 8, 7, 6, 5, 4, 3, 2, 0, 1, 111]
	>>> dc = distributed_container(input_container, 3)
	>>> dc.sort()
	[0, 1, 2, 3, 4, 5, 6, 7, 8, 88, 111]
	>>> input_container = [3, 2, 1]
	>>> dc = distributed_container(input_container, 3)
	>>> dc.sort()
	[1, 2, 3]
	>>> input_container = [3, 2, 1]
	>>> dc = distributed_container(input_container, 2)
	>>> dc.sort()
	[1, 2, 3]

	>>> input_container = [8, 7, 6, 5, 4, 3, 2, 1]
	>>> dc = distributed_container(input_container, 4, 4)
	>>> dc.sort()
	[1, 2, 3, 4, 5, 6, 7, 8]
	>>> input_container = [88, 8, 7, 6, 5, 4, 3, 2, 0, 1, 111]
	>>> dc = distributed_container(input_container, 4, 4)
	>>> dc.sort()
	[0, 1, 2, 3, 4, 5, 6, 7, 8, 88, 111]
	>>> input_container = [88, 8, 7, 6, 5, 4, 3, 2, 0, 1, 111]
	>>> dc = distributed_container(input_container, 3, 4)
	>>> dc.sort()
	[0, 1, 2, 3, 4, 5, 6, 7, 8, 88, 111]
	>>> input_container = [3, 2, 1]
	>>> dc = distributed_container(input_container, 3, 3)
	>>> dc.sort()
	[1, 2, 3]
	>>> input_container = [3, 2, 1]
	>>> dc = distributed_container(input_container, 2, 2)
	>>> dc.sort()
	[1, 2, 3]

	>>> # 'TESTS_OF:self.min'
	>>> input_container = [1, 2, 3, 4, 5, 6, 7, 8]
	>>> dc = distributed_container(input_container, 4)
	>>> dc.min()
	1
	>>> dc = distributed_container(input_container, 4, 4)
	>>> dc.min()
	1

	>>> # 'TESTS_OF:self.max'
	>>> input_container = [1, 2, 3, 4, 5, 6, 7, 8]
	>>> dc = distributed_container(input_container, 4)
	>>> dc.max()
	8
	>>> dc = distributed_container(input_container, 4, 4)
	>>> dc.max()
	8

	>>> # 'TESTS_OF:self.bytearray'
	>>> input_container = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
	>>> dc = distributed_container(input_container, 4)
	>>> tmp_res = dc.bytearray()
	>>> str(tmp_res) == '\\x00\\x01\\x02\\x03\\x04\\x05\\x06\\x07\\x08\\t'
	True
	"""

	def __init__(self, container, chunks=2, workers=None):
		"""
		:type container: list or tuple
		:type workers: chunks
		:type workers: int

		:rtype: str or list

		container - data to process
		workers - amount of workers to run
		"""

		if isinstance(container, list):
			self.container_type = 'list'
		elif isinstance(container, tuple):
			self.container_type = 'tuple'
		else:
			raise Exception("Incorrect type of container: ({}), expected: 'list' or 'tuple'.".format(type(container)))

		self.container = container

		
		if not isinstance(chunks, int):
			raise Exception("Wrong type of third parameter(workers): ({}), expected: int".format(type(chunks)))

		if workers is not None and not isinstance(workers, int):
			raise Exception("Wrong type of forth parameter(workers): ({}), expected: int".format(type(workers)))

		
		self.container_length = len(container)

		if self.container_length < workers or self.container_length < chunks:
			raise Exception('Amount of workers/chunks cannot be higher than elements in container')

		if workers is not None and workers < 2:
			raise Exception('Amount of workers cannot be lower than 2')

		self.workers = workers

		if chunks < 2:
			raise Exception('Amount of chunks cannot be lower than 2')

		self.chunks = chunks

		from multiprocessing.pool import ThreadPool
		self.ThreadPool = ThreadPool
		
		from sys import _getframe
		self._getframe = _getframe

		from collections import defaultdict
		self.defaultdict = defaultdict

		self.sliced_scopes = self.__count_slices__()


		if self.workers == None:
			self.tp = self.ThreadPool()
		else:
			self.tp = self.ThreadPool(self.workers)


	def __keyword_parser__(self, key=None):
		"""
		Internal generic method to parse keyword arguments.
		"""

		kwargs = {
				 'key':key
				 }

		if key == None:
			kwargs.pop('key')
		
		return kwargs

	
	def __count_slices__(self):
		"""
		Internal method to calculate slices
		"""
		split_sizes = int(self.container_length / self.chunks)
		remains = self.container_length - split_sizes * self.chunks

		scopes = []

		for i in range(0, self.chunks * split_sizes, split_sizes):
			scopes.append([i, i + split_sizes])
		scopes[-1][-1] += remains

		return [slice(start, stop) for start, stop in scopes]


	def count(self, element_to_find):
		"""
		Function gives the same result like classic `count` method but is implemented on many threads.
		"""

		result = self.tp.map(lambda slc: self.container[slc].count(element_to_find), self.sliced_scopes)

		return sum(result)


	def __indexes_worker__(self, slc, element_to_find, s):
		"""
		Internal method of library to searching for indexes.
		"""
		result = []
		while True:
			try:
				result.append(slc.index(element_to_find))
			except ValueError:
				break
			else:
				slc[result[-1]] = None
				result[-1] += s

		return result


	def indexes(self, element_to_find):
		"""
		Method return indexes of `element_to_find`, works on many threads.
		"""

		results = self.tp.map(lambda slc: self.__indexes_worker__(self.container[slc], element_to_find, slc.start), self.sliced_scopes)

		all_results = []
		for n, one_result in enumerate(results):
			while len(one_result) > 0:
				all_results.append(one_result.pop(0))

		return all_results


	def __remove_all_worker__(self, lst, elements_to_remove):
		"""
		Internal method of the library
		"""

		elements_to_remove = elements_to_remove[:]

		while len(elements_to_remove) > 0:

			while True:
				try:
					lst.remove(elements_to_remove[-1])
				except ValueError:
					break

			elements_to_remove.pop()
		return lst


	def remove_all(self, elements_to_remove):
		"""
		Method works only on lists.
		"""

		if self.container_type != 'list':
			raise Exception('Method: ({}) works only on lists'.format(self._getframe().f_code.co_name))

		if not isinstance(elements_to_remove, (list, tuple)):
			raise Exception("Method: ({}), wrong type of parameter: ({}), expected: 'list' or 'tuple'".format(self._getframe().f_code.co_name, type(elements_to_remove)))

		results = self.tp.map(lambda slc: self.__remove_all_worker__(self.container[slc], elements_to_remove), self.sliced_scopes)

		return [y for x in results for y in x]


	def __sort_worker__(self, lst):
		"""
		Internal method
		"""
		d = self.defaultdict(lambda: 0)
		i = len(lst)
		while i > 0:
			d[lst.pop()] += 1
			i -= 1
		
		return d


	def sort(self):
		"""
		Sorting based on dict.
		"""

		results = self.tp.map(lambda slc: self.__sort_worker__(self.container[slc]), self.sliced_scopes)

		res_dct = self.defaultdict(lambda: 0)

		for element in results:
			for key in element:
				res_dct[key] += element[key]

		res_arr = []
		for key in sorted(res_dct.keys()):
			res_arr += [key] * res_dct[key]

		return res_arr


	def min(self, key=None):
		"""
		Works the same like builtin min function.
		"""
		kwargs = self.__keyword_parser__(key=key)

		results = self.tp.map(lambda slc: min(self.container[slc], **kwargs), self.sliced_scopes)

		return min(results)


	def max(self, key=None):
		"""
		Works the same like builtin max function.
		"""
		kwargs = self.__keyword_parser__(key=key)

		results = self.tp.map(lambda slc: max(self.container[slc], **kwargs), self.sliced_scopes)

		return max(results)


	def bytearray(self):
		"""
		Works the same like builtin bytearray function.
		"""

		results = self.tp.map(lambda slc: bytearray(self.container[slc]), self.sliced_scopes)

		return bytearray().join(results)


if __name__ == '__main__':

	"""
	SAMPLE TEST OF A SINGLE FUNCTION FROM SHELL:
	
	1. lauch on the very begin:
	import distributed_containers;dc = distributed_containers.distributed_container([1,2,3,4], 2)

	2. reload after changes:
	reload(distributed_containers);dc = distributed_containers.distributed_container([1,2,3,4], 2)

	3. run ongoing:
	dc.remove_all([1,2])
	"""

	import doctest

	from random import randint
	from time import time

	doctest.testmod(verbose=True, optionflags=doctest.ELLIPSIS)	
	
	print "\n   ::: Efficiency TESTS :::   \n"
	
	print "1) .count on list(or tuple)"
	print 'Generating data...'
	input_container = [randint(5, 15) for x in range(30000000)]
	print 'A) distributed_container:',
	start_time = time()
	container = distributed_container(input_container, 16, 2)
	container.count(10)
	print time() - start_time, 'seconds'

	print 'B) standard method:',
	start_time = time()
	input_container.count(10)
	print time() - start_time, 'seconds'
	
	print "\n2) .indexes in list(or tuple)"
	print 'Generating data...'
	input_container = [randint(5, 15) for x in range(100000)]
	print 'A) distributed_container:',
	start_time = time()
	container = distributed_container(input_container, 64, 2)
	container.indexes(10)
	print time() - start_time, 'seconds'

	print 'B) one thread way:',
	start_time = time()

	indexes_res = []
	to_find = 10

	while True:
		try:
			indexes_res.append(input_container.index(to_find))
		except ValueError:
			break
		else:
			input_container[indexes_res[-1]] = None

	print time() - start_time, 'seconds'
	
	print '\n   ::Since version 0.2::   '
	
	print '\n3) .remove_all from list'
	print 'Generating data...'
	input_container = [randint(5, 15) for x in range(100000)]
	print 'A) distributed_container:',
	start_time = time()
	container = distributed_container(input_container, 20, 2)
	v_0 = container.remove_all([7, 11])
	print time() - start_time, 'seconds'

	print 'B) one thread way:',
	lst = input_container[:]
	elements_to_remove = [7, 11]
	start_time = time()

	while len(elements_to_remove) > 0:
		while True:
			try:
				lst.remove(elements_to_remove[-1])
			except ValueError:
				break

		elements_to_remove.pop()
	print time() - start_time, 'seconds'

	print '\n4) .sort list'
	print 'Generating data...'
	input_container = [randint(1, 101) for x in range(10000000)]
	input_container_2 = input_container[:]
	print 'A) distributed_container:',
	start_time = time()
	container = distributed_container(input_container, 2, 2)
	container = container.sort()
	print time() - start_time, 'seconds'

	print 'B) one thread way:',
	start_time = time()
	input_container_2.sort()
	print time() - start_time, 'seconds'
	
	print '\n   ::Since version 0.3::   '
	print '\n5) .min'
	print 'Generating data...'
	input_container = [1,2,3,4,5,6,7,8]*6000000

	print 'A) distributed_container:',
	start_time = time()
	container = distributed_container(input_container, 32, 64)
	container = container.min()
	print time() - start_time, 'seconds'

	print 'B) one thread way:',
	start_time = time()
	min(input_container)
	print time() - start_time, 'seconds'

	print '\n6) .max'

	print 'A) distributed_container:',
	start_time = time()
	container = distributed_container(input_container, 32, 64)
	container = container.max()
	print time() - start_time, 'seconds'

	print 'B) one thread way:',
	start_time = time()
	max(input_container)
	print time() - start_time, 'seconds'

	print '\n   ::Since version 0.4::   '
	print '\n7) .bytearray'

	input_container = [randint(0, 255) for _ in range(20000000)]

	print 'A) distributed_container:',
	start_time = time()
	container = distributed_container(input_container, 256, 6)
	container = container.bytearray()
	print time() - start_time, 'seconds'

	print 'B) one thread way:',
	start_time = time()
	bytearray(input_container)
	print time() - start_time, 'seconds'

	print ''
	
