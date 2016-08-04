# rdd
类似于spark流式迭代计算的python单机实现


# RDD
RDD lib, write python code easy way



## example  word count in file

```
from .DStream import RDD

def parse_line(line):
    r = line.strip().split()
    return r

RDD.TextFile('/home/wangjinxiang/workspace/RDD/dict.txt'
        ).map(parse_line
        ).flatmap(lambda x:(x,1)
        ).reduceBykey(lambda x,y:x+y
        ).sort(key=lambda x:x[1]
        ).map(lambda x:"%s,%d\n"%(x[0], x[1])
        ).saveAsTextFile('result')
```
