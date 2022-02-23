import csv

def read_file(filename: str):
    transformed_data = []
    with open(filename, 'r') as file:   
        dict_reader = csv.DictReader(file, delimiter=',')
        for row in dict_reader:
            transformed_data.append(dict(row))
        return transformed_data

def csv_to_dict(filename):
    result_list=[]
    with open(filename) as file_obj:
        reader = csv.DictReader(file_obj, delimiter=',')
        for row in reader:
            result_list.append(dict(row))
    return result_list
    
    
# read_file("chesterfield_transactions.csv") # Has errors
# result_list = csv_to_dict("chesterfield_transactions.csv") # Works
# print(result_list)
result = read_file("chesterfield_transactions.csv")
print(result)
