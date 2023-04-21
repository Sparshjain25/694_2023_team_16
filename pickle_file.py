import pickle

# Create a dictionary to be saved to the pickle file
dic = {}

# Open the file in write binary mode
with open('cache.pickle', 'wb') as f:
    # Write the dictionary to the file
    pickle.dump(dic, f)