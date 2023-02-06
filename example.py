import pandas
import re

list = ['2022-01', '2022-02', '2022-03', '2022-04',
        '2022-05', '2022-06', '2022-07', '2022-08', '2022-09', '2022-10',
        '2022-11', '2022-12', '2023-01', '2023-02', '2023-03', '2023-04',
        ]

search_period = ['2022-01', '2022-02', '2022-03', '2022-04',
        '2022-05', '2022-06', '2022-07', '2022-08', '2022-09', '2022-10',
        '2022-11', '2022-12', '2023-01', '2023-02', '2023-03', '2023-04',
        ]
# last_period = list[len(list) - 1]
# start_year = re.findall(r'^\d{4}', last_period)[0]
# period = re.sub(r'^\d{4}', str(int(start_year) - 1), last_period)
period = '2023-02'
high = len(list) - 1
low = 0
count = 0
result = []
start_index = 0
while low <= high:
    count += 1
    index = (low + high) // 2
    value = list[index]
    if period == value:
        if list[index] != len(list)-1:
            start_index = index + 1
            break
    elif value > period:
        high = index - 1
    else:
        low = index + 1

if start_index != 0:
    for i in list[start_index:]:
        result.append(i)
print(result)
