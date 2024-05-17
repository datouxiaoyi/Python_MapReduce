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