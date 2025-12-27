file_path = "underlying_keys.txt"

with open(file_path, "r") as f:
    values_list = [line.strip() for line in f]

print(values_list)


# with open(file_path, "r") as f:
#     values_list = [line.strip() for line in f if line.strip()]




