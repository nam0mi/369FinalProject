# import pandas as pd
# def main():
#     df = pd.read_csv('flights.csv', dtype={'COLUMN_NAME': str}, low_memory=False)
#     df['DAY_OF_WEEK'] = df['DAY_OF_WEEK'].map({1: 'M',
#                                            2: 'Tu',
#                                            3: 'W',
#                                            4: 'Th',
#                                            5: 'F',
#                                            6: 'Sa',
#                                            7: 'Su'})

#     threshold = 5000  # Adjust this value as needed

# # Calculate value counts for each airport in both ORIGIN_AIRPORT and DESTINATION_AIRPORT
#     origin_counts = df['ORIGIN_AIRPORT'].value_counts()
#     destination_counts = df['DESTINATION_AIRPORT'].value_counts()

#     # Find airports that appear less frequently than the threshold
#     less_frequent_origins = origin_counts[origin_counts <= threshold].index.tolist()
#     less_frequent_destinations = destination_counts[destination_counts <= threshold].index.tolist()

#     # Replace less frequent airports in 'ORIGIN_AIRPORT' with 'OTHER'
#     df['ORIGIN_AIRPORT'] = df['ORIGIN_AIRPORT'].apply(lambda x: 'OTHER' if x in less_frequent_origins else x)

#     # Replace less frequent airports in 'DESTINATION_AIRPORT' with 'OTHER'
#     df['DESTINATION_AIRPORT'] = df['DESTINATION_AIRPORT'].apply(lambda x: 'OTHER' if x in less_frequent_destinations else x)

#     for col in df.columns:
#         if(col not in ['DEPARTURE_DELAY','DAY_OF_WEEK','AIRLINE', 'ORIGIN_AIRPORT', 'DESTINATION_AIRPORT']):
#             df.drop(col, axis=1, inplace=True)

#     # Optionally, select and rename columns as needed, similar to your Scala processing
#     # flights_encoded = ...
#     flights_encoded = pd.get_dummies(df, columns=['DAY_OF_WEEK','AIRLINE', 'ORIGIN_AIRPORT', 'DESTINATION_AIRPORT'])
#     # Save the processed DataFrame to a new CSV file
#     flights_encoded.to_csv('src/flights_encoded.csv', index=False)


# main()
# import pandas as pd

# def main():
#     df = pd.read_csv('flights.csv', dtype={'COLUMN_NAME': str}, low_memory=False)
#     df['DAY_OF_WEEK'] = df['DAY_OF_WEEK'].map({1: 'M',
#                                                2: 'Tu',
#                                                3: 'W',
#                                                4: 'Th',
#                                                5: 'F',
#                                                6: 'Sa',
#                                                7: 'Su'})

#     threshold = 5000  # Adjust this value as needed

#     # Calculate value counts for each airport in both ORIGIN_AIRPORT and DESTINATION_AIRPORT
#     origin_counts = df['ORIGIN_AIRPORT'].value_counts()
#     destination_counts = df['DESTINATION_AIRPORT'].value_counts()

#     # Find airports that appear less frequently than the threshold
#     less_frequent_origins = origin_counts[origin_counts <= threshold].index.tolist()
#     less_frequent_destinations = destination_counts[destination_counts <= threshold].index.tolist()

#     # Replace less frequent airports in 'ORIGIN_AIRPORT' and 'DESTINATION_AIRPORT' with 'OTHER'
#     df['ORIGIN_AIRPORT'] = df['ORIGIN_AIRPORT'].apply(lambda x: 'OTHER' if x in less_frequent_origins else x)
#     df['DESTINATION_AIRPORT'] = df['DESTINATION_AIRPORT'].apply(lambda x: 'OTHER' if x in less_frequent_destinations else x)

#     # Drop unnecessary columns
#     for col in df.columns:
#         if col not in ['DEPARTURE_DELAY', 'DAY_OF_WEEK', 'AIRLINE', 'ORIGIN_AIRPORT', 'DESTINATION_AIRPORT']:
#             df.drop(col, axis=1, inplace=True)
    

#     # Encode categorical variables
#     #flights_encoded = pd.get_dummies(df, columns=['DAY_OF_WEEK', 'AIRLINE', 'ORIGIN_AIRPORT', 'DESTINATION_AIRPORT'])

#     # Sample 10% of the data
#     #flights_encoded_sample = flights_encoded.sample(frac=0.1, random_state=1)  # Use a fixed random_state for reproducibility

#     print(df['ORIGIN_AIRPORT'].value_counts().head(21))
#     print(df['DESTINATION_AIRPORT'].value_counts().head(21))
#     # Save the sampled DataFrame to a new CSV file
#     #flights_encoded_sample.to_csv('src/flights_encoded_sample.csv', index=False)

# main()

import pandas as pd

def main():
    df = pd.read_csv('flights.csv', dtype={'COLUMN_NAME': str}, low_memory=False)
    df['DAY_OF_WEEK'] = df['DAY_OF_WEEK'].map({1: 'M',
                                               2: 'Tu',
                                               3: 'W',
                                               4: 'Th',
                                               5: 'F',
                                               6: 'Sa',
                                               7: 'Su'})

    # Drop unnecessary columns first to make the DataFrame lighter for the next operations
    relevant_columns = ['DEPARTURE_DELAY', 'DAY_OF_WEEK', 'AIRLINE', 'ORIGIN_AIRPORT', 'DESTINATION_AIRPORT']
    df = df[relevant_columns]

    # Define the list of airports to keep
    airports_to_keep = ['ATL', 'ORD', 'DFW', 'DEN', 'LAX', 'SFO', 'PHX', 'IAH', 'LAS', 'MSP', 'MCO', 'SEA', 'DTW', 'BOS', 'EWR', 'CLT', 'LGA', 'SLC', 'JFK', 'BWI']

    # Filter the DataFrame to only include rows where both 'ORIGIN_AIRPORT' and 'DESTINATION_AIRPORT' are in the airports_to_keep list
    df = df[df['ORIGIN_AIRPORT'].isin(airports_to_keep) & df['DESTINATION_AIRPORT'].isin(airports_to_keep)]

    # The rest of the processing can continue from here, such as encoding categorical variables if needed
    flights_encoded = pd.get_dummies(df, columns=['DAY_OF_WEEK', 'AIRLINE', 'ORIGIN_AIRPORT', 'DESTINATION_AIRPORT'])
    flights_encoded_sample = flights_encoded.sample(frac=0.1, random_state=1)  # Use a fixed random_state for reproducibility
    flights_encoded.to_csv('src/flights_encoded_condensed.csv', index=False)
    flights_encoded_sample.to_csv('src/flights_encoded_small_sample.csv', index=False)
    # If further processing is required or if you want to sample or encode the data, you can continue from here.

main()
