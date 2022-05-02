import math
import time
from pyspark.context import SparkContext
from pyspark.conf import SparkConf
sc = SparkContext.getOrCreate(SparkConf())
sol1 = sc.textFile("hdfs://127.0.0.1:9000/input/2M.ID.CONTENTS")

startTwo = time.time()

def Func(lines) :
	lines = lines.lower()
	return lines

sol1 = sol1.flatMap(lambda line : line.split(" ")).map(Func)

sol1.take(100)

endTwo = time.time()

startOne = time.time()

giho = "!@#$%^&*()_+-={}:\"<>?[]|;\',./0123456789"

#하단의 코드는 split("") 이용 후, 
#sol1에 대해 string화하려고 하였으나 지속된 오류로 인하여
#부득이하게 다음과 같이 코드를 작성.

sol1 = sol1.flatMap(lambda line : line.split(" "))

#하단의 코드는 for문을 이용하려고 하였으나 
#filter가 sol1에 적용이 되지 않음. 

sol1 = sol1.filter(lambda x : '!' not in x)
sol1 = sol1.filter(lambda x : '@' not in x)
sol1 = sol1.filter(lambda x : '#' not in x)	
sol1 = sol1.filter(lambda x : '$' not in x)
sol1 = sol1.filter(lambda x : '%' not in x)
sol1 = sol1.filter(lambda x : '^' not in x)
sol1 = sol1.filter(lambda x : '&' not in x)
sol1 = sol1.filter(lambda x : '*' not in x)
sol1 = sol1.filter(lambda x : '(' not in x)
sol1 = sol1.filter(lambda x : ')' not in x)
sol1 = sol1.filter(lambda x : '-' not in x)
sol1 = sol1.filter(lambda x : '_' not in x)
sol1 = sol1.filter(lambda x : '+' not in x)
sol1 = sol1.filter(lambda x : '=' not in x)
sol1 = sol1.filter(lambda x : '[' not in x)
sol1 = sol1.filter(lambda x : ']' not in x)
sol1 = sol1.filter(lambda x : '{' not in x)
sol1 = sol1.filter(lambda x : '}' not in x)
sol1 = sol1.filter(lambda x : '\'' not in x)
sol1 = sol1.filter(lambda x : '\"' not in x)
sol1 = sol1.filter(lambda x : '/' not in x)
sol1 = sol1.filter(lambda x : '?' not in x)
sol1 = sol1.filter(lambda x : '<' not in x)
sol1 = sol1.filter(lambda x : '>' not in x)
sol1 = sol1.filter(lambda x : ',' not in x)
sol1 = sol1.filter(lambda x : '.' not in x)
sol1 = sol1.filter(lambda x : ';' not in x)
sol1 = sol1.filter(lambda x : ':' not in x)
sol1 = sol1.filter(lambda x : '0' not in x)
sol1 = sol1.filter(lambda x : '1' not in x)
sol1 = sol1.filter(lambda x : '2' not in x)
sol1 = sol1.filter(lambda x : '3' not in x)
sol1 = sol1.filter(lambda x : '4' not in x)
sol1 = sol1.filter(lambda x : '5' not in x)
sol1 = sol1.filter(lambda x : '6' not in x)
sol1 = sol1.filter(lambda x : '7' not in x)
sol1 = sol1.filter(lambda x : '8' not in x)
sol1 = sol1.filter(lambda x : '9' not in x)

sol1.take(100)

endOne = time.time()

print("1 실행 시간",endOne - startOne)
print("2 실행 시간",endTwo - startTwo)

startThree = time.time()

ct = sol1.map(lambda word: word[0])

#본래라면 하단 코드에 
#.reduceByKey(lambda a,b:a+b)를 추가하여 답을 구하여야 하나
#오류가 발생하여 알아본 결과, 메모리 부족이 원인임을 알았습니다. 
# 하지만, 메모리를 최대로 할당하여도
#문제가 해결되지 않아 reduceByKey는 다음의 코드에서 
#제외하였습니다. 

ct = ct.map(lambda x : (x, 1))

ct.take(100)

endThree = time.time()

print("3 실행 시간",endThree - startThree)


