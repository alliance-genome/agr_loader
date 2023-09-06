    def get_generators(self, filepath, batch_size):  # noqa
        """Get Generators."""

        OBOHelper.add_metadata_to_neo(filepath)
        o_data = TXTFile(filepath).get_data()
        parsed_line = OBOHelper.parse_obo(o_data)

        counter = 0

        terms = []
        syns = []
        isas = []
        partofs = []
        subsets = []
        altids = []

        for line in parsed_line:  # Convert parsed obo term into a schema-friendly AGR dictionary.

            counter += 1
            o_syns = line.get('synonym')
            ident = line['id'].strip()
            prefix = ident.split(":")[0]
            display_synonym = ""
            o_altids = line.get('alt_id')

            if o_altids is not None:
                if isinstance(o_altids, (list, tuple)):
                    for altid in o_altids:
                        alt_dict_to_append = {
                            'primary_id': ident,
                            'secondary_id': altid
                        }
                        altids.append(alt_dict_to_append)
                else:
                    alt_dict_to_append = {
                        'primary_id': ident,
                        'secondary_id': o_altids
                    }
                    altids.append(alt_dict_to_append)

            if o_syns is not None:
                if isinstance(o_syns, (list, tuple)):
                    for syn in o_syns:

                        synsplit = re.split(r'(?<!\\)"', syn)
                        syns_dict_to_append = {
                            'oid': ident,
                            'syn': synsplit[1].replace('\\"', '""')
                        }
                        syns.append(syns_dict_to_append)  # Synonyms appended here.
                        if "DISPLAY_SYNONYM" in syn:
                            display_synonym = synsplit[1].replace('"', '""')
                else:
                    synsplit = re.split(r'(?<!\\)"', o_syns)
                    syns_dict_to_append = {
                        'oid': ident,
                        'syn': synsplit[1].replace('\"', '""')
                    }
                    syns.append(syns_dict_to_append)  # Synonyms appended here.
                    if "DISPLAY_SYNONYM" in o_syns:
                        display_synonym = synsplit[1].replace('\"', '""')
            # subset
            new_subset = line.get('subset')
            subsets.append(new_subset)

            # is_a processing
            o_is_as = line.get('is_a')
            if o_is_as is not None:
                if isinstance(o_is_as, (list, tuple)):
                    for isa in o_is_as:
                        if 'gci_filler=' not in isa:
                            isa_without_name = isa.split(' ')[0].strip()
                            isas_dict_to_append = {
                                'oid': ident,
                                'isa': isa_without_name}
                            isas.append(isas_dict_to_append)
                else:
                    if 'gci_filler=' not in o_is_as:
                        isa_without_name = o_is_as.split(' ')[0].strip()
                        isas_dict_to_append = {'oid': ident,
                                               'isa': isa_without_name}
                        isas.append(isas_dict_to_append)

            # part_of processing
            relations = line.get('relationship')
            if relations is not None:
                if isinstance(relations, (list, tuple)):
                    for partof in relations:
                        if 'gci_filler=' not in partof:
                            relationship_descriptors = partof.split(' ')
                            o_part_of = relationship_descriptors[0]
                            if o_part_of == 'part_of':
                                partof_dict_to_append = {
                                    'oid': ident,
                                    'partof': relationship_descriptors[1]
                                }
                                partofs.append(partof_dict_to_append)
                else:
                    if 'gci_filler=' not in relations:
                        relationship_descriptors = relations.split(' ')
                        o_part_of = relationship_descriptors[0]
                        if o_part_of == 'part_of':
                            partof_dict_to_append = {
                                'oid': ident,
                                'partof': relationship_descriptors[1]}
                            partofs.append(partof_dict_to_append)

            definition = line.get('def')
            if definition is None:
                definition = ""
            else:
                # Looking to remove instances of \" in the definition string.
                if "\\\"" in definition:
                    # Replace them with just a single "
                    definition = definition.replace('\\\"', '\"')

            if definition is None:
                definition = ""

            is_obsolete = line.get('is_obsolete')
            if is_obsolete is None:
                is_obsolete = "false"

            if ident is None or ident == '':
                self.logger.warning("Missing oid.")
            else:
                term_dict_to_append = {
                    'name': line.get('name'),
                    'name_key': line.get('name'),
                    'oid': ident,
                    'definition': definition,
                    'is_obsolete': is_obsolete,
                    'oPrefix': prefix,
                    'oboFile': prefix,
                    'o_type': line.get('namespace'),
                    'display_synonym': display_synonym
                }

                terms.append(term_dict_to_append)

            # Establishes the number of genes to yield (return) at a time.
            if counter == batch_size:
                counter = 0
                yield [terms, isas, partofs, syns, altids]
                terms = []
                syns = []
                isas = []
                partofs = []

        if counter > 0:
            yield [terms, isas, partofs, syns, altids]
