import logging, os, time
from pyfaidx import Fasta
from common import ContextInfo

logger = logging.getLogger(__name__)


class AssemblySequenceHelper(object):
    def __init__(self, assembly, data_manager):
        sub_type = assembly.replace('.', '').replace('_', '')
        fasta_config = data_manager.get_config('FASTA')
        for sub_type_config in fasta_config.get_sub_type_objects():
            if not sub_type == sub_type_config.get_sub_data_type():
                continue
            else:
                filepath = sub_type_config.get_filepath()
                break
        if filepath is None:
            logger.warning("Can't find Assembly filepath for %s" % assembly)
            exit(3)

        self.assembly = assembly
        self.filepath = filepath
        fa = Fasta(filepath)
        while len(fa.keys()) == 0:
            time.sleep(6)
            os.remove(filepath + ".fai")
            fa = Fasta(filepath)
        self.fa = fa

    def getAssembly(self):
        return self.assembly

    def getFilepath(self):
        return self.filepath

    def getChromosomes(self):
        return self.fa.keys()

    def getSequence(self, chromosome, first, second):
        if first > second:
           start = second
           end = first
        else:
           start = first
           end = second

        start = start - 1
        if chromosome in self.fa:
            return self.fa[chromosome][start:end].seq
        else:
            logger.warning("Chromosome " + chromosome + " not in assembly " + self.assembly)
