#!/usr/bin/env python
# -*- encoding: utf-8 -*-

__author__ = 'andyguo'

import zmq
import sys
import time
import random
import threading
from util import tprint




class LocalAsyncServer(object):
    def __init__(self, port=18765, worker=4):
        self.port = port
        self.worker = worker

    def run(self):
        from worker import LocalAsyncWorker

        # 配置socket
        context = zmq.Context.instance()
        front_end = context.socket(zmq.ROUTER)
        front_end.bind('tcp://*:{}'.format(self.port))
        back_end = context.socket(zmq.DEALER)
        back_end.bind('inproc://dayu_local_async_server')

        # 启动worker 线程
        for x in range(self.worker):
            worker = LocalAsyncWorker(x)
            tt = threading.Thread(target=worker.run)
            tt.start()

        # 创建poller，并注册socket
        poller = zmq.Poller()
        poller.register(front_end, zmq.POLLIN)
        poller.register(back_end, zmq.POLLIN)

        while True:
            sockets = dict(poller.poll(500))
            if front_end in sockets:
                message = front_end.recv_multipart()
                client_address = message[0]

                # 如果前端传入的消息 == DAYU_KILL, 那么表示需要退出
                if message[-1] == 'DAYU_KILL':
                    # 发送和worker 数量相同的DAYU_KILL 信号
                    for x in range(self.worker):
                        back_end.send_multipart([client_address, 'DAYU_KILL'])

                    exit_workers = 0
                    # 等待接收worker 正常退出时候，返回的 DAYU_WORKER_EXIT 信号
                    while exit_workers < self.worker:
                        replay_message = back_end.recv_multipart()
                        # 接收到的worker 消息，可能是正常退出的信号，也可能是之前没有完成任务的消息，需要分开处理
                        if replay_message[-1] == 'DAYU_WORKER_EXIT':
                            tprint(str(replay_message))
                            exit_workers += 1
                        else:
                            front_end.send_multipart(replay_message)

                    # 接收到所有worker 都返回正常的退出信号后，向client 发送整个server 退出的信号
                    front_end.send_multipart([client_address, 'DAYU_SERVER_EXIT'])
                    break
                else:
                    back_end.send_multipart(message)

            # 正常的后端进度信号，向前端传递
            if back_end in sockets:
                front_end.send_multipart(back_end.recv_multipart())

        # 给client 一点点时间反应
        time.sleep(0.1)


if __name__ == '__main__':
    server = LocalAsyncServer()
    server.run()
