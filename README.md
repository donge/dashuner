# 股票数据采集与存储系统

这个项目是一个用于采集和存储股票数据的Python脚本。它从adata API获取股票数据，并将其存储在PostgreSQL数据库中。

## 主要功能

1. 获取所有股票代码
2. 创建必要的数据库表
3. 获取每只股票的K线数据
4. 将数据插入或更新到PostgreSQL数据库

## 数据库表结构

### stock_codes 表

| 字段名 | 类型 | 描述 |
|--------|------|------|
| stock_code | VARCHAR(20) | 股票代码（主键） |
| short_name | VARCHAR(50) | 股票简称 |
| exchange | VARCHAR(20) | 交易所 |
| list_date | VARCHAR(10) | 上市日期 |

### stock_market_kline 表

| 字段名 | 类型 | 描述 |
|--------|------|------|
| stock_code | VARCHAR(20) | 股票代码 |
| trade_date | DATE | 交易日期 |
| trade_time | TIMESTAMP | 交易时间 |
| open | NUMERIC | 开盘价 |
| close | NUMERIC | 收盘价 |
| high | NUMERIC | 最高价 |
| low | NUMERIC | 最低价 |
| volume | BIGINT | 成交量 |
| amount | NUMERIC | 成交额 |
| change | NUMERIC | 涨跌额 |
| change_pct | NUMERIC | 涨跌幅 |
| turnover_ratio | NUMERIC | 换手率 |

注：stock_code、trade_date 和 trade_time 共同构成主键。

## 使用说明

1. 确保已安装所需的Python库：adata、psycopg2
2. 配置PostgreSQL数据库连接信息
3. 运行脚本开始数据采集和存储过程

请注意，该脚本会自动处理数据更新，避免重复插入相同的数据。

