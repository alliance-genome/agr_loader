def parseGO(data):
    ontologyData = []
    go_dict = {}
    seenFirstTerm = False

    for line in data:
        if len(line) == 0: # Move past blank lines
            continue
        elif line == '[TERM]':
            seenFirstTerm = True
            if go_dict: # If go_dict has data.
                ontologyData.append(go_dict)
            go_dict.clear()
        else:
            if seenFirstTerm is True:
                go_dict = process_line(line, go_dict)

    ontologyData.append(go_dict) # Append last entry.

    return ontologyData

def process_line(line, go_dict):
    k, v = line.strip().split(':')
    if go_dict[k]:
        temp_value = go_dict[k]
        go_dict[k] = [temp_value, v]
    else:
        go_dict[k] = v

    return go_dict