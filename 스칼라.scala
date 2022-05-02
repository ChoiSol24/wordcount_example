spark-shell --conf spark.driver.maxResultSize=5g --driver-memory 6g

var sol1 = sc.textFile("hdfs://127.0.0.1:9000/input/2M.ID.CONTENTS")
val startOne = System.currentTimeMillis
var giho = "!@#$%^&*()1234567890_+-={}[]:\";\'<>?,./"
var gihoArr = giho.split("")
//원래 하단의 코드를 split("")로 처리 후, 기호를 제거한 것을
//collect()로 array화 하여 mkString을 이용해 다시 wordcount를 
//진행하려고 하였으나, 메모리를 최대로 설정하여도
//계속되는 java heap space 에러로 인하여 부득이하게
//wordcount를 실행하기 위하여 코드를 다음과 같이 작성하였습니다.  
sol1 = sol1.flatMap(_.split(" "))
for (i <- gihoArr) {
sol1 = sol1.filter(!_.contains(i))}
sol1.take(100)
val endOne = System.currentTimeMillis
println("1 수행 시간"+((endOne - startOne)/1000.0f) +"sec") 
val startTwo = System.currentTimeMillis
sol1 = sol1.map(x => x.toLowerCase)
sol1.take(100)
val endTwo = System.currentTimeMillis
println("2 수행 시간"+((endTwo - startTwo)/1000.0f) +"sec")
val startThree = System.currentTimeMillis
var ct = sol1.map(word => (word.take(1),1)).reduceByKey(_+_)
ct.take(100)
val endThree = System.currentTimeMillis
println("3 수행 시간" +((endThree - startThree)/1000.0f) +"sec")
