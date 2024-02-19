

list_data = ["telo", "joko", "opo"]

datas = f"{list_data[0]}"
for data in list_data[1:]:
    datas = f"{datas},{data}"

print(datas)