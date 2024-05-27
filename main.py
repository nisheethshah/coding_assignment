import os
import pandas as pd
from datetime import datetime
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi


def read_data(file_path):
    """
    Reads the CSV file from the given file path and returns a DataFrame.

    :param file_path: str, path to the CSV file
    :return: DataFrame, pandas DataFrame containing the CSV data
    """
    try:
        # Read the CSV file
        data = pd.read_csv(file_path, delimiter='|', header=None)

        # Define column names
        column_names = [
            'FirstName', 'LastName', 'Company', 'BirthDate', 'Salary',
            'Address', 'Suburb', 'State', 'Post', 'Phone', 'Mobile', 'Email'
        ]

        # Assign column names
        data.columns = column_names

        return data
    except Exception as e:
        print(f"Error reading CSV file: {e}")
        return None


def transform_data(data):
    """
    Transforms the data by performing various cleaning and formatting tasks.

    :param data: DataFrame, the raw data
    :return: DataFrame, the cleaned and transformed data
    """
    # Convert string columns to string type and strip whitespace
    string_columns = ['FirstName', 'LastName', 'Company', 'Address', 'Suburb', 'State', 'Email']
    for column in string_columns:
        data[column] = data[column].astype(str).str.strip()

    # Manually parse the 'BirthDate' column
    def parse_birthdate(birthdate):
        birthdate_str = str(birthdate).strip().replace('.0', '')
        if len(birthdate_str) == 8:
            day = birthdate_str[:2]
            month = birthdate_str[2:4]
            year = birthdate_str[4:]
        elif len(birthdate_str) == 7:
            day = birthdate_str[:1]
            month = birthdate_str[1:3]
            year = birthdate_str[3:]
        else:
            return None  # Return None if the format is unexpected

        # Ensure the day and month are always two digits
        day = day.zfill(2)
        month = month.zfill(2)

        try:
            return f"{day}/{month}/{year}"
        except ValueError:
            return None

    data['BirthDate'] = data['BirthDate'].apply(parse_birthdate)

    # Ensure the 'BirthDate' column is in the DD/MM/YYYY format
    data['BirthDate'] = pd.to_datetime(data['BirthDate'], format='%d/%m/%Y', errors='coerce').dt.strftime('%d/%m/%Y')

    # Merge FirstName and LastName into FullName
    data['FullName'] = data['FirstName'] + ' ' + data['LastName']

    # Calculate Age as of March 1st, 2024
    reference_date = datetime.today()
    data['Age'] = (reference_date - pd.to_datetime(data['BirthDate'], format='%d/%m/%Y')).dt.days // 365

    # Calculate the average age
    average_age = data['Age'].mean()

    # Set missing BirthDate based on the average age
    avg_birthdate = (reference_date - pd.to_timedelta(average_age * 365, unit='D')).replace(month=1, day=1)
    data['BirthDate'] = data['BirthDate'].fillna(avg_birthdate.strftime('%d/%m/%Y'))

    # Recalculate Age for the imputed BirthDate
    data['Age'] = (reference_date - pd.to_datetime(data['BirthDate'], format='%d/%m/%Y',
                                                   errors='coerce')).dt.days // 365

    # Format Salary with commas and dollar sign
    data['Salary'] = data['Salary'].apply(lambda x: f"${x:,.2f}")

    # Categorize Salary into SalaryBucket
    def categorize_salary(salary):
        salary = float(salary.replace('$', '').replace(',', ''))
        if salary < 50000:
            return 'A'
        elif 50000 <= salary <= 100000:
            return 'B'
        else:
            return 'C'

    data['SalaryBucket'] = data['Salary'].apply(categorize_salary)

    # Drop columns FirstName and LastName
    data.drop(columns=['FirstName', 'LastName'], inplace=True)

    return data


def load_data(data, mongo_uri, db_name, collection_name):
    """
    Loads the transformed data into a MongoDB database.

    :param data: DataFrame, the transformed data
    :param mongo_uri: str, MongoDB connection URI
    :param db_name: str, name of the database
    :param collection_name: str, name of the collection
    """
    try:
        # Convert DataFrame to a list of dictionaries
        records = data.to_dict(orient='records')

        # Connect to MongoDB
        client = MongoClient(mongo_uri, server_api=ServerApi('1'), tlsAllowInvalidCertificates=True)

        # Send a ping to confirm a successful connection
        try:
            client.admin.command('ping')
            print("Pinged your deployment. You successfully connected to MongoDB!")
        except Exception as e:
            print(f"Error pinging MongoDB: {e}")
            return

        db = client[db_name]
        collection = db[collection_name]

        # Drop the collection if it exists
        collection.drop()

        # Insert records into the collection
        collection.insert_many(records)

        print("Data loaded successfully into the MongoDB database.")
    except Exception as e:
        print(f"Error loading data into MongoDB: {e}")


def main():
    csv_file_path = 'member-data.csv'

    # Read the MongoDB URI from the environment variable
    mongo_uri = os.getenv('MONGO_URI')
    db_name = 'coding_assignment'
    collection_name = 'employees'

    data = read_data(csv_file_path)

    if data is not None:
        transformed_data = transform_data(data)
        load_data(transformed_data, mongo_uri, db_name, collection_name)
        transformed_data.to_excel('output.xlsx', index=False)

    print("Done!!")


if __name__ == '__main__':
    main()