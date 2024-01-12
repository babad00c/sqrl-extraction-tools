import os
import pandas as pd

def extract_info_from_filename(filename):
    # Extracting the followed_user part of the filename
    # Filename format: twitter-followingXXXXXXXXXXXXX-followed_user.xlsx
    parts = filename.split('-')
    if len(parts) >= 3:
        followed_user = parts[-1].split('.')[0]  # Get the last part and remove '.xlsx'
        return followed_user
    return None

def process_files(directory):
    # List to hold all dataframes
    all_dataframes = []

    # Iterate through matching files in the directory
    for filename in os.listdir(directory):
        if filename.endswith('.xlsx') and 'twitter-following' in filename:
            # Extract followed_user from the filename
            followed_user = extract_info_from_filename(filename)
            if followed_user:
                # Construct the full file path
                file_path = os.path.join(directory, filename)
                
                # Read the excel file into a dataframe
                df = pd.read_excel(file_path, header=6)

                # Check if 'username' column exists in the DataFrame
                if 'Username' in df.columns:
                    # Copy values from 'username' column to 'source' column
                    df['source'] = df['Username']

                # Add the 'followed_user' and 'target' columns
                df['followed_user'] = followed_user
                df['target'] = followed_user
                
                # Append the dataframe to the list
                all_dataframes.append(df)

    # Combine all dataframes into a single dataframe
    combined_df = pd.concat(all_dataframes, ignore_index=True)
    output_df = combined_df[['source', 'target', 'Followers']]

    # Export the cropped dataframe to a CSV file
    output_df.to_csv('twitter-graph.csv', index=False)

    return combined_df

def main():
    process_files('/Users/zachary.henson/Downloads')

if __name__ == '__main__':
    main()
