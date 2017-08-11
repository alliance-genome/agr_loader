from neo4j.v1 import GraphDatabase
from .transaction import Transaction

class DiseaseTransaction(Transaction):

    def __init__(self, graph):
        Transaction.__init__(self, graph)


    def disease_object_tx(self, data):
        '''
        Loads the Disease data into Neo4j.
        Nodes: merge object (gene, genotype, transgene, allele, etc..., merge disease term,
        '''

        geneQuery = """

            UNWIND $data as row

            //   GENE  ***********

            FOREACH (x IN CASE WHEN row.diseaseObjectType = 'gene' THEN [1] ELSE [] END |
                //TODO: test if adding "DiseaseObject" label breaks merge
                MERGE (f:Gene {primaryKey:row.primaryId})

                MERGE (f)<-[:FROM_SPECIES]->(spec)
                SET f.with = row.with

                MERGE (d:DOTerm {primaryKey:row.doId})
                SET d.doDisplayId = row.doDisplayId
                SET d.doUrl = row.doUrl
                SET d.doPrefix = row.doPrefix

                FOREACH (rel IN CASE when row.relationshipType = 'is_marker_for' THEN [1] ELSE [] END |
                    MERGE (f)<-[fa:IS_MARKER_FOR]->(d))

                FOREACH (rel IN CASE when row.relationshipType = 'is_implicated_in' THEN [1] ELSE [] END |
                    MERGE (f)<-[fa:IS_IMPLICATED_IN]->(d))

                FOREACH (qualifier IN CASE when row.qualifier = 'NOT' and row.relationshipType = 'is_marker_of' THEN [1] ELSE [] END |
                    MERGE (f)<-[fq:IS_NOT_MARKER_OF]->(d))

                 FOREACH (qualifier IN CASE when row.qualifier = 'NOT' and row.relationshipType = 'is_implicated_in' THEN [1] ELSE [] END |
                    MERGE (f)<-[fq:IS_NOT_IMPLICATED_IN]->(d))


                //Create the Association node to be used for the object/doTerm
                MERGE (da:Annotation {primaryKey:row.diseaseAssociationId, link_from:row.primaryId, link_to:row.doId})

                //Create the relationship from the object node to association node.
                //Create the relationship from the association node to the DoTerm node.

                MERGE (f)-[fda:ASSOCIATION]->(da)
                MERGE (da)-[dad:ASSOCIATION]->(d)

                MERGE (pub:Publication {primaryKey:row.pubPrimaryKey})
                SET pub.pubModId = row.pubModId
                SET pub.pubMedId = row.pubMedId
                SET pub.pubModUrl = row.pubModUrl
                SET pub.pubMedUrl = row.pubMedUrl

                MERGE (da)-[dapu:ANNOTATED_TO]->(pub)

                FOREACH (entity in row.evidenceCodes|
                        MERGE (ecode1:EvidenceCode {primaryKey:entity})
                        MERGE (da)-[daecode1:ANNOTATED_TO]->(ecode1)
                )
                MERGE (pub)-[pubEv:ANNOTATED_TO]->(ecode1)
                MERGE (da)-[dapa:ANNOTATED_TO]->(pub)

            )

        """

        genotypeQuery = """

            UNWIND $data as row
            //   GENOTYPE  ***********

            FOREACH (x IN CASE WHEN row.diseaseObjectType = 'genotype' THEN [1] ELSE [] END |

                MERGE (f:Genotype:DiseaseObject {primaryKey:row.primaryId})

                FOREACH (entity in row.ecodes|
                    MERGE (gecode:EvidenceCode {primaryKey:entity}))

                MERGE (f)<-[:FROM_SPECIES]->(spec)
                SET f.with = row.with

                MERGE (d:DOTerm {primaryKey:row.doId})
                SET d.doDisplayId = row.doDisplayId
                SET d.doUrl = row.doUrl
                SET d.doPrefix = row.doPrefix

                FOREACH (rel IN CASE when row.relationshipType = 'is_model_of' THEN [1] ELSE [] END |
                    MERGE (f)<-[fa:IS_MODEL_OF]->(d))
                FOREACH (qualifier IN CASE when row.qualifier = 'NOT' and row.relationshipType = 'is_model_of' THEN [1] ELSE [] END |
                    MERGE (f)<-[fq:IS_NOT_MODEL_OF]->(d))

                MERGE (gda:Association {primaryKey:row.diseaseAssociationId, link_from:row.primaryId, link_to:row.doId})

                //Create the relationship from the object node to association node.
                //Create the relationship from the association node to the DoTerm node.
                MERGE (f)-[fda:ASSOCIATION]->(da)
                MERGE (da)-[dad:ASSOCIATION]->(d)

                //Create nodes for other identifiers.  TODO- do this better. evidence code node needs to be linked up with each
                //of these separately.

                MERGE (pub:Publication {primaryKey:row.pubPrimaryKey})
                SET pub.pubModId = row.pubModId
                SET pub.pubMedId = row.pubMedId
                SET pub.pubModUrl = row.pubModUrl
                SET pub.pubMedUrl = row.pubMedUrl

                FOREACH (entity in row.evidenceCodes|
                        MERGE (ecode1:EvidenceCode {primaryKey:entity})
                        MERGE (gda)-[ecode1e:ANNOTATED_TO]->(ecode1)
                        MERGE (gda)-[dae:ANNOTATED_TO]->(ecode1)
                )
                MERGE (pub)-[pubEv:ANNOTATED_TO]->(ecode1)
                MERGE (gda)-[gdapa:ANNOTATED_TO]->(pub)

                //inferred from gene
                MERGE (ig:Gene {primaryKey:row.inferredGene})
                MERGE (ig)<-[igg:INFERRED]->(f)

            )
        """

        alleleQuery = """

            UNWIND $data as row
            //   ALLELE  ***********

            FOREACH (x IN CASE WHEN row.diseaseObjectType = 'allele' THEN [1] ELSE [] END |
                MERGE (f:Allele {primaryKey:row.primaryId})

                MERGE (f)<-[:FROM_SPECIES]->(spec)
                SET f.with = row.with

                MERGE (aig:Gene {primaryKey:row.inferredGene})
                MERGE (aig)<-[aigg:INFERRED]->(f)

                MERGE (d:DOTerm {primaryKey:row.doId})
                SET d.doDisplayId = row.doDisplayId
                SET d.doUrl = row.doUrl
                SET d.doPrefix = row.doPrefix

                FOREACH (rel IN CASE when row.relationshipType = 'is_marker_for' THEN [1] ELSE [] END |
                    MERGE (f)<-[fa:IS_MARKER_FOR]->(d))

                FOREACH (rel IN CASE when row.relationshipType = 'is_implicated_in' THEN [1] ELSE [] END |
                    MERGE (f)<-[fa:IS_IMPLICATED_IN]->(d))

                FOREACH (qualifier IN CASE when row.qualifier = 'NOT' and row.relationshipType = 'is_marker_of' THEN [1] ELSE [] END |
                    MERGE (f)<-[fq:IS_NOT_MARKER_OF]->(d))

                FOREACH (qualifier IN CASE when row.qualifier = 'NOT' and row.relationshipType = 'is_implicated_in' THEN [1] ELSE [] END |
                    MERGE (f)<-[fq:IS_NOT_IMPLICATED_IN]->(d))

                MERGE (pub:Publication {primaryKey:row.pubPrimaryKey})
                SET pub.pubModId = row.pubModId
                SET pub.pubMedId = row.pubMedId
                SET pub.pubModUrl = row.pubModUrl
                SET pub.pubMedUrl = row.pubMedUrl

                FOREACH (entity in row.evidenceCodes|
                        MERGE (ecode1:EvidenceCode {primaryKey:entity})
                        MERGE (ada)-[adaecode1:ANNOTATED_TO]->(ecode1)
                        MERGE (ada)-[adaae:ANNOTATED_TO]->(ecode1)
                )

            )
        """

        transgeneQuery = """
            UNWIND $data as row

            //   TRANSGENE  ***********


            FOREACH (x IN CASE WHEN row.diseaseObjectType = 'transgene' THEN [1] ELSE [] END |
                MERGE (f:Transgene {primaryKey:row.primaryId})

                MERGE (ig:Gene {primaryKey:row.inferredGene})
                MERGE (ig)-[igg:INFERRED]->(f)

                FOREACH (rel IN CASE when row.relationshipType = 'is_marker_for' THEN [1] ELSE [] END |
                    MERGE (f)<-[fa:IS_MARKER_FOR]->(d))

                FOREACH (rel IN CASE when row.relationshipType = 'is_implicated_in' THEN [1] ELSE [] END |
                    MERGE (f)<-[fa:IS_IMPLICATED_IN]->(d))

                FOREACH (qualifier IN CASE when row.qualifier = 'NOT' and row.relationshipType = 'is_marker_of' THEN [1] ELSE [] END |
                    MERGE (f)<-[fq:IS_NOT_MARKER_OF]->(d))

                 FOREACH (qualifier IN CASE when row.qualifier = 'NOT' and row.relationshipType = 'is_implicated_in' THEN [1] ELSE [] END |
                    MERGE (f)<-[fq:IS_NOT_IMPLICATED_IN]->(d))
            )
        """

        fishQuery = """
            UNWIND $data as row

            //    FISH  ************

            FOREACH (x IN CASE WHEN row.diseaseObjectType = 'fish' THEN [1] ELSE [] END |

                //fish
                MERGE (f:Fish {primaryKey:row.primaryId})
                SET f.name = row.diseaseObjectName

                //Fish environments!
                MERGE(fenv:Fish:Environment:DiseaseObject {primaryKey:row.fishEnvId})
                MERGE(fenv)-[ffenv:ANNOTATED_TO]-(f)

                FOREACH (condition in row.experimentalConditions |
                    MERGE (env:EnvironmentCondition {primaryKey: condition})
                    MERGE (fenv)-[ef:ANNOTATED_TO]->(env)
                )

                MERGE (fig:Gene {primaryKey:row.inferredGene})
                MERGE (fig)<-[figg:INFERRED]->(f)

                //species
                MERGE (f)<-[:FROM_SPECIES]->(spec)
                SET f.with = row.with

                //diseaseTerm
                MERGE (d:DOTerm {primaryKey:row.doId})
                SET d.doDisplayId = row.doDisplayId
                SET d.doUrl = row.doUrl
                SET d.doPrefix = row.doPrefix
                MERGE (d)-[dfenv:ANNOTATED_TO]->(fenv)

                //Create the Association node to be used for the object/doTerm
                MERGE (fida:Association {linkTo:row.fishEnvId, linkFrom:row.doId})
                MERGE (fida)-[fenva:ANNOTATED_TO]->(fenv)

                MERGE (fpub:Publication {primaryKey:row.pubPrimaryKey})
                SET fpub.pubModId = row.pubModId
                SET fpub.pubMedId = row.pubMedId
                SET fpub.pubModUrl = row.pubModUrl
                SET fpub.pubMedUrl = row.pubMedUrl

                FOREACH (entity in row.evidenceCodes|
                        MERGE (ecode1:EvidenceCode {primaryKey:entity})
                        MERGE (fida)-[ecode1e:ANNOTATED_TO]->(ecode1)
                        MERGE (fida)-[dae:ANNOTATED_TO]->(ecode1)
                )

                FOREACH (rel IN CASE when row.relationshipType = 'is_model_of' THEN [1] ELSE [] END |
                    MERGE (fenv)<-[fenvd:IS_MODEL_OF]->(d)
                    MERGE (fenv)<-[fenvda:ANNOTATED_TO]->(d)
                )

                FOREACH (qualifier IN CASE when row.qualifier = 'NOT' and row.relationshipType = 'is_model_of' THEN [1] ELSE [] END |
                    MERGE (fenv)<-[fenvq:IS_NOT_MODEL_OF]->(d)
                    MERGE (fenv)<-[fenvqa:ANNOTATED_TO]->(d)
                )
            )


        """
        Transaction.execute_transaction(self, geneQuery, data)
        Transaction.execute_transaction(self, genotypeQuery, data)
        Transaction.execute_transaction(self, alleleQuery, data)
        Transaction.execute_transaction(self, transgeneQuery, data)
        Transaction.execute_transaction(self, fishQuery, data)
