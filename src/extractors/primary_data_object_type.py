from enum import Enum


class PrimaryDataObjectType(Enum):
    gene = 1
    allele = 2
    strain = 3
    genotype = 4
    feature = 5