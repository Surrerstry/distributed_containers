__author__ = 'Surrerstry'
__version__ = '0.2'
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

	>>> # 'TESTS_OF:self.remove_all'
	>>> dc = distributed_container([1, 2, 3, 4, 1, 2, 1], 2)
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
	"""

	def __init__(self, container, workers=2):
		"""
		:type container: list or tuple
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

		if not isinstance(workers, int):
			raise Exception("Wrong type of third parameter(workers): ({}), expected: int".format(type(workers)))

		if len(container) < workers:
			raise Exception('Amount of workers cannot be higher than elements in container')

		self.container_length = len(container)

		if workers < 2:
			raise Exception('Amount of workers cannot be lower than 2')

		self.workers = workers

		from multiprocessing.pool import ThreadPool
		self.ThreadPool = ThreadPool
		
		from sys import _getframe
		self._getframe = _getframe

		from collections import defaultdict
		self.defaultdict = defaultdict


	def __count_slices__(self):
		"""
		Internal method to calculate slices
		"""
		split_sizes = int(self.container_length / self.workers)
		remains = self.container_length - split_sizes * self.workers

		scopes = []

		for i in range(0, self.workers * split_sizes, split_sizes):
			scopes.append([i, i + split_sizes])
		scopes[-1][-1] += remains

		return [slice(start, stop) for start, stop in scopes]


	def count(self, element_to_find):
		"""
		Function gives the same result like classic `count` method but is implemented on many threads.
		"""

		sliced_scopes = self.__count_slices__()

		tp = self.ThreadPool()

		result = tp.map(lambda slc: self.container[slc].count(element_to_find), sliced_scopes)

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

		sliced_scopes = self.__count_slices__()

		tp = self.ThreadPool()

		results = tp.map(lambda slc: self.__indexes_worker__(self.container[slc], element_to_find, slc.start), sliced_scopes)

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

		sliced_scopes = self.__count_slices__()

		tp = self.ThreadPool()

		results = tp.map(lambda slc: self.__remove_all_worker__(self.container[slc], elements_to_remove), sliced_scopes)

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

		sliced_scopes = self.__count_slices__()

		tp = self.ThreadPool()

		results = tp.map(lambda slc: self.__sort_worker__(self.container[slc]), sliced_scopes)

		res_dct = self.defaultdict(lambda: 0)

		for element in results:
			for key in element:
				res_dct[key] += element[key]

		res_arr = []
		for key in sorted(res_dct.keys()):
			res_arr += [key] * res_dct[key]

		return res_arr


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
	input_container = [randint(5, 15) for x in range(40000000)]
	print 'A) distributed_container:',
	start_time = time()
	container = distributed_container(input_container, 24)
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
	container = distributed_container(input_container, 64)
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
	container = distributed_container(input_container, 16)
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
	container = distributed_container(input_container, 4)
	container = container.sort()
	print time() - start_time, 'seconds'

	print 'B) one thread way:',
	start_time = time()
	input_container_2.sort()
	print time() - start_time, 'seconds'
	print ''
