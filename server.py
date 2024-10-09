from SimpleXMLRPCServer import SimpleXMLRPCServer, SimpleXMLRPCRequestHandler
import os
import sys
from threading import Lock

# 定义常量，表示未处理的请求错误代码
UNHANDLED = 100


# 定义线程锁以确保并发安全
class Node:
    def __init__(self, url, dirname, secret):
        self.secret = secret  # 用于身份验证的密钥
        self.dirname = dirname  # 服务端文件所在的目录
        self.known = set()  # 用于存储已知的其他节点 URL
        self.lock = Lock()  # 确保线程安全
        # 启动 XML-RPC 服务器
        self.server = SimpleXMLRPCServer(('localhost', int(url.split(':')[-1])),
                                         requestHandler=SimpleXMLRPCRequestHandler, logRequests=False)
        self.server.register_instance(self)  # 注册本实例的方法供远程调用

    # 启动服务器
    def _start(self):
        print(f'Server started on {self.server.server_address}')
        self.server.serve_forever()  # 开始监听并处理客户端请求

    # 处理 hello 消息，存储其他已知节点的 URL
    def hello(self, other):
        with self.lock:
            print(f"Hello received from {other}")
            self.known.add(other)  # 添加其他节点的 URL

    # 处理文件获取请求
    def fetch(self, filename, secret):
        if secret != self.secret:  # 检查身份验证密钥
            raise Fault(UNHANDLED, "Unauthorized access")

        filepath = os.path.join(self.dirname, filename)  # 构建文件路径
        if not os.path.isfile(filepath):
            raise Fault(UNHANDLED, "File not found")  # 文件未找到

        try:
            with open(filepath, 'rb') as f:
                return f.read()  # 返回文件内容
        except IOError:
            raise Fault(UNHANDLED, "File could not be read")  # 处理文件读取错误

    # 停止服务器（可选）
    def stop(self):
        self.server.shutdown()
        print("Server stopped.")


# 启动服务器的主函数
if __name__ == '__main__':
    if len(sys.argv) != 4:
        print(f"Usage: {sys.argv[0]} <url> <directory> <secret>")
        sys.exit(1)

    url = sys.argv[1]  # 服务端 URL (例如 'http://localhost:8000')
    dirname = sys.argv[2]  # 服务端存储文件的目录
    secret = sys.argv[3]  # 服务端的密钥

    # 创建并启动 Node 实例
    n = Node(url, dirname, secret)
    try:
        n._start()  # 启动 XML-RPC 服务器
    except KeyboardInterrupt:
        print("\nServer shutting down.")
        n.stop()
