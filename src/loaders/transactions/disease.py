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


        ###  start of the object -generic- query sections ####

        unwindQuery = """
            UNWIND $data as row

        """

        doTermQuery = """

            MERGE (d:DOTerm:Ontology {primaryKey:row.doId})
                SET d.doDisplayId = row.doDisplayId
                SET d.doUrl = row.doUrl
                SET d.doPrefix = row.doPrefix
                SET d.doId = row.doId

        """

        speciesQuery = """
             MERGE (spec:Species {primaryKey: row.taxonId})
             MERGE (f)<-[:FROM_SPECIES]->(spec)
             ON CREATE SET f.with = row.with

        """
        #TODO: handle null cases in inferredFrom

        inferredFromGeneQuery = """

            FOREACH (ifg in CASE WHEN row.inferredGene IS NULL THEN [] ELSE [1] END |
                FOREACH (inferred in row.inferredGene |
                    MERGE(ig:Gene {primaryKey: inferred})
                    MERGE(ig)<-[igg:INFERRED_FROM]->(f)
                )
            )
            """
        environmentQuery = """

            FOREACH (condition in row.experimentalConditions |
                MERGE (env:EnvironmentCondition {primaryKey: condition})
                ON CREATE SET env.name = row.condition
                MERGE (fenv)-[ef:ANNOTATED_TO]->(env)
            )


        """

        additionalGeneticComponentsQuery = """

                WITH row.additionalGeneticComponents as components
                UNWIND components as component
                //TODO: do we need to type these nodes? For now, just one big multi-typed node.  Needs to be broken down to types, but need type in disease schema/submission first.
                MERGE (ent:Entity {primaryKey: component.componentId})
                ON CREATE SET ent.name = component.componentSymbol
                ON CREATE SET ent.componentUrl = component.componentUrl

        """
        pubQuery = """

         MERGE (pub:Publication {primaryKey:row.pubPrimaryKey})
                ON CREATE SET pub.pubModId = row.pubModId
                ON CREATE SET pub.pubMedId = row.pubMedId
                ON CREATE SET pub.pubModUrl = row.pubModUrl
                ON CREATE SET pub.pubMedUrl = row.pubMedUrl

        MERGE (da:Annotation {primaryKey:row.diseaseAssociationId, link_from:row.primaryId, link_to:row.doId})
        MERGE (da)-[dapu:ANNOTATED_TO]->(pub)

        FOREACH (entity in row.ecodes|
                MERGE (ecode1:EvidenceCode {primaryKey:entity})
                MERGE (da)-[daecode1:ANNOTATED_TO]->(ecode1)
                MERGE (pub)-[pubEv:ANNOTATED_TO]->(ecode1)
        )

        """

        ###  start of the object -specific- query sections ####
        geneQuery = """


            FOREACH (x IN CASE WHEN row.diseaseObjectType = 'gene' THEN [1] ELSE [] END |
                //TODO: test if adding "DiseaseObject" label breaks merge
                MERGE (f:Gene {primaryKey:row.primaryId})
                ON CREATE SET f.name = row.diseaseObjectName

                //the foreach statments that represent relationships/associationType are not here for capitalization purposes.
                //instead we mimic a conditional statement by creating a collection with 1 value, and an empty collection.

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

            )

        """

        genotypeQuery = """

            FOREACH (x IN CASE WHEN row.diseaseObjectType = 'genotype' THEN [1] ELSE [] END |
                MERGE (f:Genotype {primaryKey:row.primaryId})
                ON CREATE SET f.name = row.diseaseObjectName

                FOREACH (rel IN CASE when row.relationshipType = 'is_model_of' THEN [1] ELSE [] END |
                    MERGE (f)<-[fa:IS_MODEL_OF]->(d))
                FOREACH (qualifier IN CASE when row.qualifier = 'NOT' and row.relationshipType = 'is_model_of' THEN [1] ELSE [] END |
                    MERGE (f)<-[fq:IS_NOT_MODEL_OF]->(d))

                MERGE (da:Association {primaryKey:row.diseaseAssociationId, link_from:row.primaryId, link_to:row.doId})

                //Create the relationship from the object node to association node.
                //Create the relationship from the association node to the DoTerm node.
                MERGE (f)-[fda:ASSOCIATION]->(da)
                MERGE (da)-[dad:ASSOCIATION]->(d)

            )
        """

        alleleQuery = """


            //   ALLELE  ***********

            FOREACH (x IN CASE WHEN row.diseaseObjectType = 'allele' THEN [1] ELSE [] END |
                MERGE (f:Allele {primaryKey:row.primaryId})
                ON CREATE SET f.name = row.diseaseObjectName


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

        transgeneQuery = """


            //   TRANSGENE  ***********


            FOREACH (x IN CASE WHEN row.diseaseObjectType = 'transgene' THEN [1] ELSE [] END |
                MERGE (f:Transgene {primaryKey:row.primaryId})
                ON CREATE SET f.name = row.diseaseObjectName


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


            //    FISH  ************

            FOREACH (x IN CASE WHEN row.diseaseObjectType = 'fish' THEN [1] ELSE [] END |

                //fish
                MERGE (f:Fish {primaryKey:row.primaryId})
                ON CREATE SET f.name = row.diseaseObjectName

                //Fish environments!
                MERGE(fenv:Fish:Environment:DiseaseObject {primaryKey:row.fishEnvId})
                MERGE(fenv)-[ffenv:ANNOTATED_TO]-(f)

                FOREACH (condition in row.experimentalConditions |
                    MERGE (env:EnvironmentCondition {primaryKey: condition})
                    SET env.name = row.condition
                    MERGE (fenv)-[ef:ANNOTATED_TO]->(env)
                )


                //Create the Association node to be used for the object/doTerm
                MERGE (fida:Association {linkTo:row.fishEnvId, linkFrom:row.doId})
                MERGE (fida)-[fenva:ANNOTATED_TO]->(fenv)


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
        #TODO: add back inferredFromGene query - with checks to handle null cases.

        executeGene = unwindQuery + speciesQuery + doTermQuery + geneQuery + pubQuery
        executeGenotype = unwindQuery + speciesQuery + doTermQuery + genotypeQuery + inferredFromGeneQuery + pubQuery + environmentQuery
        executeAllele = unwindQuery + speciesQuery + doTermQuery + alleleQuery + pubQuery + inferredFromGeneQuery + environmentQuery + additionalGeneticComponentsQuery
        executeTransgene = unwindQuery + speciesQuery + doTermQuery + transgeneQuery + pubQuery + inferredFromGeneQuery + environmentQuery
        executeFish = unwindQuery + speciesQuery + doTermQuery + fishQuery + pubQuery + inferredFromGeneQuery

        Transaction.execute_transaction(self, executeGenotype, data)
        Transaction.execute_transaction(self, executeGene, data)
        Transaction.execute_transaction(self, executeAllele, data)
        Transaction.execute_transaction(self, executeTransgene, data)
        Transaction.execute_transaction(self, executeFish, data)
