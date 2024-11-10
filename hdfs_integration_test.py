from hdfs import InsecureClient

# Replace guest_machine_ip with the IP address of your Hadoop cluster on the guest machine
client = InsecureClient('http://192.168.1.14:9870', user='hduser')

# List files in the root directory
files = client.list('/user/yogov/')
print(files)

# Upload a file
client.upload('/user/yogov/livy_test.py', '/home/yogesh/Documents/AI_Engineer/Product Development/Livy_Check/livy_test.py')
