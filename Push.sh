#!/bin/bash

git add *
git commit -m "$(date "+%Y-%m-%d %H:%M:%S")"
git push -u origin main

#使运行窗口，出错保留，成功关闭
if [ $? -ne 0 ]; then
  exec /bin/bash
fi

#打开新终端，保留窗口
# exec /bin/bash
