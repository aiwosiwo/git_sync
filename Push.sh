#!/bin/bash
# 推送到github/gitee代码仓库
git add *
git commit -m "$(date "+%Y-%m-%d %H:%M:%S")"
git push -u origin

#使运行窗口，出错保留，成功关闭
if [ $? -ne 0 ]; then
  exec /bin/bash
fi

#打开新终端，保留窗口
# exec /bin/bash
