import csv
from datetime import datetime

delimited_files = [
    'data/202004-divvy-tripdata.csv',
    'data/202005-divvy-tripdata.csv',
    'data/202006-divvy-tripdata.csv',
    'data/202007-divvy-tripdata.csv',
    'data/202008-divvy-tripdata.csv',
    'data/202009-divvy-tripdata.csv',
    'data/202010-divvy-tripdata.csv',
    'data/202011-divvy-tripdata.csv',
    'data/202012-divvy-tripdata.csv',
    'data/202101-divvy-tripdata.csv',
    'data/202102-divvy-tripdata.csv',
    'data/202103-divvy-tripdata.csv'
]


def convert_csv_to_fixed_width(files: list):
    with open('fixed_width_data.txt', 'w') as f:
        for file in files:
            rows = read_with_csv(file)
            for row in rows:
                num_rows = range(13)
                str_line = ''
                for num in num_rows:
                    value = row[num]
                    pad_value = value+'                                                   '
                    str_line += pad_value[:45]
                f.write(str_line+'\n')


def read_with_csv(file_loc: str):
    with open(file_loc) as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        for row in csv_reader:
            yield row


def read_file_return_records(file_loc: str) -> iter:
    with open(file_loc) as fixed_width_file:
        lines = fixed_width_file.readlines()
        return lines


def slice_line(line: str):
    c1 = line[:45].strip()
    c2 = line[45:90].strip()
    c3 = line[90:135].strip()
    c4 = line[135:180].strip()
    c5 = line[180:225].strip()
    c6 = line[225:270].strip()
    c7 = line[270:315].strip()
    c8 = line[315:360].strip()
    c9 = line[360:405].strip()
    c10 = line[405:450].strip()
    c11 = line[450:495].strip()
    c12 = line[495:540].strip()
    c13 = line[540:585].strip()
    return c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13


def main():
    t1 = datetime.now()
    with open('result.csv', 'w') as f2:
        with open('fixed_width_data.txt', 'r') as f:
            lines = f.readlines()
            for line in lines:
                c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13 = slice_line(line)
                f2.write(f"{c1}, {c2}, {c3}, {c4}, {c5}, {c6}, {c7}, {c8}, {c9}, {c10}, {c11}, {c12}, {c13} \n")
    t2 = datetime.now()
    x = t2 - t1
    print(f"it took {x} to process file")


if __name__ == '__main__':
    main()