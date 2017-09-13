def parseOBO(data):
    ontologyData = []
    go_dict = {}
    withinTerm = False
    withinTypedef = False

    # Ignores withinTypedef entries.

    for line in data:
        if '[Term]' in line:
            withinTerm = True
            if go_dict: # If go_dict has data (from pervious [Term]) add it to the list first.
                ontologyData.append(go_dict)
                go_dict = {} # New empty dict.
            else:
                continue
        elif '[Typedef]' in line:
            withinTypedef = True # Used for skipping data.
        else:
            if withinTerm is True:
                go_dict, withinTerm = process_line(line, go_dict, withinTerm) # Process the line.
            elif withinTypedef is True: # Skip Typedefs, look for empty line.
                if len(line.strip()) == 0:
                    withinTypedef = False # Reset withinTypedef
                else:
                    continue # Keep looking for the blank line to indicate the end of an entry.
            else:
                continue # If we hit blank lines or nonsensical lines, keep going. Skips stuff at top of file.

    ontologyData.append(go_dict) # Append last entry.

    return ontologyData # Return the list of dicts.

def process_line(line, go_dict, withinTerm):
    if len(line.strip()) == 0: # If the line is blank, reset withinTerm and kick it back.
        withinTerm = False
        return go_dict, withinTerm # The go_dict should be fully populated at this point.
    else:
        k, v = line.strip().split(':', 1) # Split the lines on the first ':'
        if k in go_dict:
            if (type(go_dict[k]) is str): # If it's an entry with a single string, turn it into a list.
                temp_value = go_dict[k]
                go_dict[k] = [temp_value, v]
            elif (type(go_dict[k]) is list): # If it's already a list, append to it.
                go_dict[k].append(v)
        else:
            go_dict[k] = v # If it's the first time we're seeing this key-value, make a new entry.

        return go_dict, withinTerm