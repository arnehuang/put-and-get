import requests
import os
import hashlib
import filecmp

# Constants
PUT_ENDPOINT = 'https://mqa2quz62ihewrjo32jf5gb2j40hkygo.lambda-url.us-west-2.on.aws/?data_id={}'
GET_ENDPOINT = 'https://r7btnkxoy2rvr32tgeptzhtqsu0tuwhw.lambda-url.us-west-2.on.aws/?data_id={}'
AUTH_HEADER = {'username': 'arne@email.com'}


def generate_data_id(file_name, chunk_index):
    # TODO: Hash collision chance is high
    # Retrieving smaller file after larger file doens't work

    # Create a unique data_id using the file name and chunk index
    hash_object = hashlib.sha256(f'{file_name}-{chunk_index}'.encode())
    # Use the first 4 characters of the hash as data_id
    return hash_object.hexdigest()[:4]


def put_data(file_path):
    # Check if the file exists and determine its size
    if not os.path.exists(file_path):
        print("File does not exist")
        return False

    file_size = os.path.getsize(file_path)
    chunk_size = 1024 * 1024  # 1MB
    chunks_count = max(1, file_size // chunk_size + (1 if file_size % chunk_size else 0))

    with open(file_path, 'rb') as file:
        for i in range(chunks_count):
            data_id = generate_data_id(os.path.basename(file_path), i)
            chunk_data = file.read(chunk_size)
            print(len(chunk_data))
            checksum = hashlib.sha256(chunk_data).hexdigest()  # Calculate checksum for the chunk
            print(f"the checksum for dataid {data_id} is {checksum}")
            chunk_with_checksum = checksum.encode() + chunk_data

            url = PUT_ENDPOINT.format(data_id)
            print(url)
            response = requests.put(url, data=chunk_with_checksum, headers=AUTH_HEADER)

            if response.status_code != 200:
                print(response.status_code)
                print(f'Failed to store chunk {i} of {file_path}')
                return False

    return True


def get_data(file_name):
    output_directory = './data/get/'
    os.makedirs(output_directory, exist_ok=True)
    output_file_path = os.path.join(output_directory, file_name)

    chunk_index = 0
    done = False
    while not done:
        retry_count = 0
        while retry_count < 10:
            retry_count += 1
            data_id = generate_data_id(file_name, chunk_index)
            url = GET_ENDPOINT.format(data_id)
            response = requests.get(url, headers=AUTH_HEADER)

            if response.status_code == 200:
                # Assume the checksum is at the beginning of the chunk and is 64 bytes long (SHA-256)
                received_data = response.content
                print(len(received_data))
                checksum_received = received_data[:64].decode()
                print(f"the received checksum for dataid {data_id} is {checksum_received}")
                chunk_data = received_data[64:]
                print(len(chunk_data))
                # Verify the checksum
                checksum_calculated = hashlib.sha256(chunk_data).hexdigest()
                print(f" the checksum calculated is {checksum_calculated}")
                if checksum_calculated == checksum_received:
                    # Append the validated chunk data to the output file
                    with open(output_file_path, 'ab') as output_file:
                        output_file.write(chunk_data)
                    chunk_index += 1
                    break  # Exit the retry loop as the data is correct
                else:
                    print(f'Data corruption detected in chunk {chunk_index} for {file_name}')
                    retry_count += 1
            if retry_count == 10:
                print(f'Failed to correct corruption in chunk {chunk_index} for {file_name} after 10 retries.')
                return False
            if response.status_code == 404:
                # No more chunks available, assuming end of file
                done = True
                break
    print(f'File retrieved successfully and saved to {output_file_path}')
    return True


# Example usage
file_name = 'pg2600.txt'
file_path = f'./data/{file_name}'
if put_data(file_path):
    print('File stored successfully')

    # Example usage for get_data

    retrieved_file_path = './data/get/' + file_name
    if get_data(file_name):
        print('File retrieved successfully')

        # Compare the original and retrieved files
        if filecmp.cmp(file_path, retrieved_file_path, shallow=False):
            print('Success: The uploaded and downloaded files are identical.')
        else:
            print('Error: The uploaded and downloaded files do not match.')
    else:
        print('Failed to retrieve file')
else:
    print('Failed to store file')
