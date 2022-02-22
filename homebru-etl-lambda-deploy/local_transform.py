import app.extract_and_transform as extract_and_transform

results = extract_and_transform.transform('chesterfield.csv')

print(type(results["products_data"]))
print(results["products_data"])