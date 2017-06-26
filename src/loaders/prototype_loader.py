from loaders import *
from annotators import *
from files import *
from mods import *
import gc
import time
import os

class AggregateLoader:

	def __init__(self):
		authenticate("localhost:7474", "neo4j", "neo4j")
		graph = Graph()

	def load_from_mods(self):
		mods = [FlyBase()]
		print("Gathering genes from each MOD.")
		for mod in mods:
			genes = mod.load_genes_prototype

	def index_data(self):
