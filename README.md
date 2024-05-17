# 一、什么是MapReduce

首先，将这个单词分解为Map、Reduce。

-   **Map**阶段：在这个阶段，输入数据集被分割成小块，并由多个Map任务处理。每个Map任务将输入数据映射为一系列(key, value)对，并生成中间结果。
-   **Reduce**阶段：在这个阶段，中间结果被重新分组和排序，以便相同key的中间结果被传递到同一个Reduce任务。每个Reduce任务将具有相同key的中间结果合并、计算，并生成最终的输出。

举个例子，在一个很长的字符串中统计某个字符出现的次数。

```
from collections import defaultdict
def mapper(word):
    return word, 1

def reducer(key_value_pair):
    key, values = key_value_pair
    return key, sum(values)
def map_reduce_function(input_list, mapper, reducer):
    '''
    - input_list: 字符列表
    - mapper: 映射函数，将输入列表中的每个元素映射到一个键值对
    - reducer: 聚合函数，将映射结果中的每个键值对聚合到一个键值对
    - return: 聚合结果
    '''
    map_results = map(mapper, input_list)
    shuffler = defaultdict(list)
    for key, value in map_results:
        shuffler[key].append(value)
    return map(reducer, shuffler.items())

if __name__ == "__main__":
    words = "python best language".split(" ")
    result = list(map_reduce_function(words, mapper, reducer))
    print(result)
```

输出结果为

> [('python', 1), ('best', 1), ('language', 1)]

但是这里并没有体现出MapReduce的特点。只是展示了MapReduce的运行原理。

# 二、基于多线程实现MapReduce

```
from collections import defaultdict
import threading

class MapReduceThread(threading.Thread):
    def __init__(self, input_list, mapper, shuffler):
        super(MapReduceThread, self).__init__()
        self.input_list = input_list
        self.mapper = mapper
        self.shuffler = shuffler

    def run(self):
        map_results = map(self.mapper, self.input_list)
        for key, value in map_results:
            self.shuffler[key].append(value)

def reducer(key_value_pair):
    key, values = key_value_pair
    return key, sum(values)
def mapper(word):
    return word, 1
def map_reduce_function(input_list, num_threads):
    shuffler = defaultdict(list)
    threads = []
    chunk_size = len(input_list) // num_threads
    
    for i in range(0, len(input_list), chunk_size):
        chunk = input_list[i:i+chunk_size]
        thread = MapReduceThread(chunk, mapper, shuffler)
        thread.start()
        threads.append(thread)
    
    for thread in threads:
        thread.join()

    return map(reducer, shuffler.items())

if __name__ == "__main__":
    words = "python is the best language for programming and python is easy to learn".split(" ")
    result = list(map_reduce_function(words, num_threads=4))
    for i in result:
        print(i)
```

这里的本质一模一样，将字符串分割为四份，并且分发这四个字符串到不同的线程执行，最后将执行结果归约。只不过由于Python的GIL机制，导致Python中的线程是并发执行的，而不能实现并行，所以在python中使用线程来实现MapReduce是不合理的。（GIL机制：抢占式线程，不能在同一时间运行多个线程）。

# 三、基于多进程实现MapReduce

由于Python中GIL机制的存在，无法实现真正的并行。这里有两种解决方案，一种是使用其他语言，例如C语言，这里我们不考虑；另一种就是利用多核，CPU的多任务处理能力。

```
from collections import defaultdict
import multiprocessing

def mapper(chunk):
    word_count = defaultdict(int)
    for word in chunk.split():
        word_count[word] += 1
    return word_count

def reducer(word_counts):
    result = defaultdict(int)
    for word_count in word_counts:
        for word, count in word_count.items():
            result[word] += count
    return result

def chunks(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

def map_reduce_function(text, num_processes):
    chunk_size = (len(text) + num_processes - 1) // num_processes
    chunks_list = list(chunks(text, chunk_size))

    with multiprocessing.Pool(processes=num_processes) as pool:
        word_counts = pool.map(mapper, chunks_list)

    result = reducer(word_counts)
    return result

if __name__ == "__main__":
    text = "python is the best language for programming and python is easy to learn"
    num_processes = 4
    result = map_reduce_function(text, num_processes)
    for i in result:
        print(i, result[i])
```

这里使用多进程来实现MapReduce，这里就是真正意义上的并行，依然是将数据切分，采用并行处理这些数据，这样才可以体现出MapReduce的高效特点。但是在这个例子中可能看不出来很大的差异，因为数据量太小。在实际应用中，如果数据集太小，是不适用的，可能无法带来任何收益，甚至产生更大的开销导致性能的下降。

  


# 四、在100GB的文件中检索数据

这里依然使用MapReduce的思想，但是有两个问题

1.  文件太大，读取速度慢

解决方法：

使用分块读取，但是在分区时不宜过小。因为在创建分区时会被序列化到进程，在进程中又需要将其解开，这样反复的序列化和反序列化会占用大量时间。不宜过大，因为这样创建的进程会变少，可能无法充分利用CPU的多核能力。

2.  文件太大，内存消耗特别大

解决方法：

使用生成器和迭代器，但需获取。例如分块为8块，生成器会一次读取一块的内容并且返回对应的迭代器，以此类推，这样就避免了读取内存过大的问题。

```
from datetime import datetime
import multiprocessing
def chunked_file_reader(file_path:str, chunk_size:int):
    """
    生成器函数：分块读取文件内容
    - file_path: 文件路径
    - chunk_size: 块大小,默认为1MB
    """
    with open(file_path, 'r', encoding='utf-8') as file:
        while True:
            chunk = file.read(chunk_size)
            if not chunk:
                break
            yield chunk

def search_in_chunk(chunk:str, keyword:str):
    """在文件块中搜索关键字
    - chunk: 文件块
    - keyword: 要搜索的关键字
    """
    lines = chunk.split('\n')
    for line in lines:
        if keyword in line:
            print(f"找到了:", line)

def search_in_file(file_path:str, keyword:str, chunk_size=1024*1024):
    """在文件中搜索关键字
    file_path: 文件路径
    keyword: 要搜索的关键字
    chunk_size: 文件块大小,为1MB
    """
    with multiprocessing.Pool() as pool:
        for chunk in chunked_file_reader(file_path, chunk_size):
            pool.apply_async(search_in_chunk, args=(chunk, keyword))
        
if __name__ == "__main__":
    start = datetime.now()
    file_path = "file.txt"
    keyword = "张三"
    search_in_file(file_path, keyword)
    end = datetime.now()
    print(f"搜索完成，耗时 {end - start}")
```

最后程序运行时间为两分钟左右。