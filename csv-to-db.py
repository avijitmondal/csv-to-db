import pandas
from sqlalchemy import create_engine
df = pandas.read_csv('./student.csv')

engine = create_engine("mysql+pymysql://root:password@localhost:3306/student?charset=utf8mb4")
df1.to_sql('student', con=engine, if_exists='append')

