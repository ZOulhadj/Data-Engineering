import csv
import random

# Number of rows of data to generate
num_rows = 100

# Open a CSV file for writing
with open('generated_data.csv', 'w', newline='') as file:
    writer = csv.writer(file)
    
    # Write the header row
    writer.writerow(["Name", "Age", "Country"])
    
    # Generate and write the data rows
    for i in range(num_rows):
        name = "Person" + str(i)
        age = random.randint(18, 99)
        country = "Country" + str(random.randint(1, 10))
        writer.writerow([name, age, country])