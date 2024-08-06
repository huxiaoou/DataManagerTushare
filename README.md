# 项目介绍

该项目主要使用tushare数据接口下载整理数据到本地。

## 用法

在project_cfg.py 文件中对下载数据保存地址，下载的数据字段，数据描述等信息进行配置。

### 下载期货日频行情数据

```powershell
    python main.py --switch fmd --bgn 20240805
```
