import csv, sys
import copy, ast

def main():
    args = ParseArgs()
    filename = args.file
    max_length = int(args.max_length)
    list_index = int(args.index)
    csv.field_size_limit(sys.maxsize)
    with open(filename, 'r') as rfile, open(f"split_{filename}", 'w', buffering=1) as wfile:
        split_list_in_file(list_index, max_length, rfile, wfile)


def split_list_in_file(list_index, max_list_length, rfile, wfile):
    reader = csv.reader(rfile)
    ln = 0
    for row in reader:
        ln = ln + 1
        list_field = ast.literal_eval(row[list_index])
        if (isinstance(list_field, str)):
            list_field = [list_field]
        list_size = len(list_field)
        if (list_size > max_list_length):
            print(f"Splitting list of size: {list_size} @ {ln}")
            start = 0
            end = start + max_list_length
            while (end < list_size):
                row_copy = copy.deepcopy(row)
                list_string = ','.join(f"\"\"{w}\"\"" for w in list_field[start:end])
                row_copy[list_index] = f"\"[{list_string}]\""
                start = start + max_list_length
                end = end + max_list_length
                wfile.write(','.join(row_copy) + "\n")
            row_copy = copy.deepcopy(row)
            list_string = ','.join(f"\"\"{w}\"\"" for w in list_field[start:end])
            row_copy[list_index] = f"\"[{list_string}]\""
            wfile.write(','.join(row_copy) + "\n")
        else:
            data = ','.join(row) + "\n"
            data = data.replace("\"","\"\"")
            data = data.replace("[","\"[")
            data = data.replace("]","]\"")
            wfile.write(data)


def ParseArgs():
    ''' Parse the arguments '''
    import argparse
    parser = argparse.ArgumentParser(
            description=f'Take as input a csv file with a field that is a long list. Every row of that csv file that contains '
                        f'a list bigger than a certain threshold. is split into multiple rows and the result is exported in a new file',
            formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('-m', '--max_length', help='Maximum list length', default=1000)
    requiredNamed = parser.add_argument_group('required named arguments')
    requiredNamed.add_argument('-f', '--file', help='CSV file that contains the long list field',
                               required=True)
    requiredNamed.add_argument('-i', '--index', help='The index of the list field (zero based)', required=True)
    args = parser.parse_args()
    return args


# %%
if __name__ == '__main__':
    main()
